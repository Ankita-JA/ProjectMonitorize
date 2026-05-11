package com.example.monitorize

import android.util.Log
import java.io.IOException
import java.net.ServerSocket
import java.net.Socket

/**
 * StreamReceiver
 *
 * Listens on TCP port 7110 for an incoming raw H.264 Annex-B stream.
 * The sender (Linux host) pipes wf-recorder | GStreamer h264parse directly —
 * so the byte stream contains standard Annex-B start codes (00 00 00 01 or 00 00 01).
 *
 * Strategy:
 *  - Buffer incoming bytes into a sliding window
 *  - Detect Annex-B start codes to delimit NAL units
 *  - On first SPS NAL, parse width/height and init decoder
 *  - Feed each complete NAL unit to H264Decoder
 *
 * Thread: runs on its own background thread; calls [onStatusChange] from that thread
 * (MainActivity posts to UI thread via runOnUiThread).
 */
class StreamReceiver(private val decoder: H264Decoder) {

    var onStatusChange: ((String) -> Unit)? = null

    private var running      = false
    private var serverSocket: ServerSocket? = null

    companion object {
        private const val TAG  = "StreamReceiver"
        const val PORT         = 7110

        // Default resolution — overridden once SPS is parsed
        private const val DEFAULT_WIDTH  = 2560
        private const val DEFAULT_HEIGHT = 1600

        // Annex-B 4-byte start code
        private val START_CODE_4 = byteArrayOf(0, 0, 0, 1)
        // Annex-B 3-byte start code
        private val START_CODE_3 = byteArrayOf(0, 0, 1)
    }

    // ── Helper for parsing H.264 bitstream ─────────────────────────────────────

    private class BitReader(private val data: ByteArray, offset: Int, private val limit: Int) {
        private var bytePtr = offset
        private var bitPtr = 0

        fun readBit(): Int {
            if (bytePtr >= limit) return 0
            val b = data[bytePtr].toInt() and 0xFF
            val bit = (b shr (7 - bitPtr)) and 1
            bitPtr++
            if (bitPtr == 8) {
                bitPtr = 0
                bytePtr++
            }
            return bit
        }

        fun readBits(n: Int): Int {
            var res = 0
            for (i in 0 until n) {
                res = (res shl 1) or readBit()
            }
            return res
        }

        fun readExpGolomb(): Int {
            var zeros = 0
            while (readBit() == 0 && zeros < 31) zeros++
            return if (zeros == 0) 0 else (1 shl zeros) - 1 + readBits(zeros)
        }

        fun readSignedExpGolomb(): Int {
            val k = readExpGolomb()
            return if (k % 2 == 0) -(k / 2) else (k + 1) / 2
        }
    }

    // ── Public API ─────────────────────────────────────────────────────────────

    fun start() {
        running = true
        Thread(::receiveLoop, "MonitorizeReceiver").start()
    }

    fun stop() {
        running = false
        try { serverSocket?.close() } catch (_: IOException) {}
        serverSocket = null
    }

    // ── Receive loop ──────────────────────────────────────────────────────────

    private fun receiveLoop() {
        try {
            serverSocket = ServerSocket(PORT)
            Log.i(TAG, "Listening on port $PORT")
            status("Waiting for stream…")

            while (running) {
                val socket: Socket = try {
                    serverSocket?.accept() ?: break
                } catch (e: IOException) {
                    if (running) Log.w(TAG, "Accept error: ${e.message}")
                    break
                }

                Log.i(TAG, "Connection from ${socket.inetAddress}")
                status("Connected — buffering…")
                handleConnection(socket)

                if (running) status("Disconnected — waiting…")
            }
        } catch (e: Exception) {
            if (running) Log.e(TAG, "Server error", e)
            status("Error: ${e.message}")
        } finally {
            Log.i(TAG, "Receive loop ended")
        }
    }

    // ── Per-connection handler ────────────────────────────────────────────────

    private fun handleConnection(socket: Socket) {
        var decoderReady = false
        var totalNals    = 0L
        
        // Large reusable buffer to avoid allocations
        val buffer = ByteArray(2 * 1024 * 1024) 
        var bufferSize = 0

        try {
            socket.use { s ->
                val input = s.getInputStream()

                while (running) {
                    val remainingSpace = buffer.size - bufferSize
                    if (remainingSpace < 4096) {
                        Log.w(TAG, "Buffer full — clearing to avoid deadlock")
                        bufferSize = 0
                    }
                    
                    val n = input.read(buffer, bufferSize, buffer.size - bufferSize)
                    if (n <= 0) break
                    bufferSize += n

                    // Find all complete NAL units in current buffer
                    var searchFrom = 0
                    while (true) {
                        val nalStart = findStartCode(buffer, searchFrom, bufferSize)
                        if (nalStart == -1) break
                        
                        val scLen = startCodeLen(buffer, nalStart, bufferSize)
                        val nalBegin = nalStart + scLen
                        
                        // Look for the NEXT start code to see if this NAL is complete
                        val next = findStartCode(buffer, nalBegin, bufferSize)
                        if (next == -1) {
                            // Incomplete NAL — stop processing and wait for more data
                            break
                        }

                        // We have a complete NAL: buffer[nalStart .. next)
                        val nalLen = next - nalBegin
                        if (nalLen > 0) {
                            val nalType = buffer[nalBegin].toInt() and 0x1F

                            if (!decoderReady && (nalType == 7 || nalType == 5)) {
                                val (w, h) = if (nalType == 7) {
                                    parseSpsResolution(buffer, nalBegin, nalLen)
                                        ?: (DEFAULT_WIDTH to DEFAULT_HEIGHT)
                                } else {
                                    DEFAULT_WIDTH to DEFAULT_HEIGHT
                                }
                                Log.i(TAG, "Initialising decoder at ${w}×${h}")
                                decoder.init(w, h)
                                decoderReady = true
                                status("Streaming ${w}×${h}")
                            }

                            if (decoderReady) {
                                decoder.decode(buffer, nalStart, next - nalStart)
                                totalNals++
                            }
                        }
                        searchFrom = next
                    }

                    // Shift remaining data to start of buffer
                    if (searchFrom > 0) {
                        val remaining = bufferSize - searchFrom
                        if (remaining > 0) {
                            System.arraycopy(buffer, searchFrom, buffer, 0, remaining)
                        }
                        bufferSize = remaining
                    }
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "Connection error", e)
        } finally {
            decoder.release()
            decoderReady = false
            Log.i(TAG, "Session ended. Total NALs delivered: $totalNals")
        }
    }

    // ── Annex-B start code utilities ──────────────────────────────────────────

    /** Returns index of first 00 00 01 or 00 00 00 01 at or after [from]. */
    private fun findStartCode(buf: ByteArray, from: Int, limit: Int): Int {
        val searchLimit = limit - 3
        var i = from
        while (i <= searchLimit) {
            if (buf[i] == 0.toByte() && buf[i + 1] == 0.toByte()) {
                if (buf[i + 2] == 1.toByte()) return i
                if (i + 3 < limit && buf[i + 2] == 0.toByte() && buf[i + 3] == 1.toByte()) return i
            }
            i++
        }
        return -1
    }

    /** Length of the start code at [pos] (3 or 4 bytes). */
    private fun startCodeLen(buf: ByteArray, pos: Int, limit: Int): Int =
        if (pos + 3 < limit && buf[pos + 2] == 0.toByte() && buf[pos + 3] == 1.toByte()) 4 else 3

    // ── SPS resolution parser (simplified, handles most x264 output) ──────────

    /**
     * Very lightweight SPS NAL parser for width/height only.
     * Handles common profiles from x264 ultrafast output.
     */
    private fun parseSpsResolution(buf: ByteArray, offset: Int, len: Int): Pair<Int, Int>? {
        return try {
            val reader = BitReader(buf, offset + 1, offset + len)
            val profileIdc = reader.readBits(8)
            reader.readBits(8) // constraint_set_flags
            reader.readBits(8) // level_idc
            reader.readExpGolomb() // seq_parameter_set_id

            if (profileIdc in listOf(100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135)) {
                val chromaFormatIdc = reader.readExpGolomb()
                if (chromaFormatIdc == 3) reader.readBit() // separate_colour_plane_flag
                reader.readExpGolomb() // bit_depth_luma_minus8
                reader.readExpGolomb() // bit_depth_chroma_minus8
                reader.readBit() // qpprime_y_zero_transform_bypass_flag
                if (reader.readBit() == 1) { // seq_scaling_matrix_present_flag
                    val count = if (chromaFormatIdc != 3) 8 else 12
                    for (i in 0 until count) {
                        if (reader.readBit() == 1) { // seq_scaling_list_present_flag
                            val size = if (i < 6) 16 else 64
                            var lastScale = 8; var nextScale = 8
                            for (j in 0 until size) {
                                if (nextScale != 0) {
                                    val deltaScale = reader.readSignedExpGolomb()
                                    nextScale = (lastScale + deltaScale + 256) % 256
                                }
                                lastScale = if (nextScale == 0) lastScale else nextScale
                            }
                        }
                    }
                }
            }
            reader.readExpGolomb() // log2_max_frame_num_minus4
            val picOrderCntType = reader.readExpGolomb()
            if (picOrderCntType == 0) {
                reader.readExpGolomb() // log2_max_pic_order_cnt_lsb_minus4
            } else if (picOrderCntType == 1) {
                reader.readBit() // delta_pic_order_always_zero_flag
                reader.readSignedExpGolomb() // offset_for_non_ref_pic
                reader.readSignedExpGolomb() // offset_for_top_to_bottom_field
                val numRefFramesInPicOrderCntCycle = reader.readExpGolomb()
                for (i in 0 until numRefFramesInPicOrderCntCycle) reader.readSignedExpGolomb()
            }
            reader.readExpGolomb() // max_num_ref_frames
            reader.readBit() // gaps_in_frame_num_value_allowed_flag
            val picWidthInMbsMinus1 = reader.readExpGolomb()
            val picHeightInMapUnitsMinus1 = reader.readExpGolomb()
            val frameMbsOnlyFlag = reader.readBit()
            if (frameMbsOnlyFlag == 0) reader.readBit() // mb_adaptive_frame_field_flag
            reader.readBit() // direct_8x8_inference_flag
            
            var cropLeft = 0; var cropRight = 0; var cropTop = 0; var cropBottom = 0
            if (reader.readBit() == 1) { // frame_cropping_flag
                cropLeft = reader.readExpGolomb()
                cropRight = reader.readExpGolomb()
                cropTop = reader.readExpGolomb()
                cropBottom = reader.readExpGolomb()
            }

            val width = (picWidthInMbsMinus1 + 1) * 16 - (cropLeft + cropRight) * 2
            val height = (2 - frameMbsOnlyFlag) * (picHeightInMapUnitsMinus1 + 1) * 16 - (cropTop + cropBottom) * 2
            
            Log.i(TAG, "Parsed SPS: ${width}x${height}")
            width to height
        } catch (e: Exception) {
            Log.e(TAG, "SPS parse error", e)
            null
        }
    }

    private fun status(msg: String) {
        onStatusChange?.invoke(msg)
    }
}
