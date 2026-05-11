#!/usr/bin/env python3
import dbus
import os
from dbus.mainloop.glib import DBusGMainLoop
from gi.repository import GLib

DBusGMainLoop(set_as_default=True)
loop = GLib.MainLoop()
bus = dbus.SessionBus()

desktop = bus.get_object('org.freedesktop.portal.Desktop', '/org/freedesktop/portal/desktop')
sc = dbus.Interface(desktop, 'org.freedesktop.portal.ScreenCast')

state = {'step': 'create_session', 'session': None}

def launch_streaming(fd, node_id):
    # Stream to tablet via adb forward on port 7110
    cmd = (
        f"gst-launch-1.0 pipewiresrc fd={fd} path={node_id} do-timestamp=true ! "
        f"queue max-size-buffers=2 ! "
        f"videoconvert n-threads=4 ! "
        f"videoscale ! "
        f"video/x-raw,format=I420,width=2560,height=1600 ! "
        f"x264enc tune=zerolatency speed-preset=ultrafast bitrate=15000 key-int-max=30 ! "
        f"h264parse config-interval=1 ! "
        f"tcpclientsink host=127.0.0.1 port=7110"
    )
    print(f"\n[STREAMING TO TABLET]: {cmd}\n")
    os.system(cmd)

def on_response(response, results, **kw):
    step = state['step']
    if step == 'create_session':
        state['session'] = str(results['session_handle'])
        state['step'] = 'select_sources'
        sc.SelectSources(state['session'], {
            'types': dbus.UInt32(1), # Monitor
            'multiple': False,
            'handle_token': dbus.String('tok2')
        })
    elif step == 'select_sources':
        state['step'] = 'start'
        sc.Start(state['session'], '', {'handle_token': dbus.String('tok3')})
    elif step == 'start':
        node_id = int(results['streams'][0][0])
        fd_obj = sc.OpenPipeWireRemote(state['session'], {})
        fd = fd_obj.take()
        print(f"Got NODE: {node_id}  FD: {fd}")
        loop.quit()
        launch_streaming(fd, node_id)

bus.add_signal_receiver(on_response, signal_name='Response',
    dbus_interface='org.freedesktop.portal.Request')

sc.CreateSession({'handle_token': dbus.String('tok1'), 'session_handle_token': dbus.String('ses1')})
loop.run()
