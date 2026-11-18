"""Hold one native JetStream PubAck until the test cancels its request.
Uses a UUID stream on the existing broker; deletes only that stream.
"""
import argparse
import json
import os
import socket
import subprocess
import threading
import uuid
import tempfile
from pathlib import Path
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('binary')
    parser.add_argument('--broker-host', default='127.0.0.1')
    parser.add_argument('--broker-port', type=int, default=4222)
    args = parser.parse_args()
    for scenario,mode in [('publisher', 'late-receipt')]:
        marker = uuid.uuid4().hex
        temporary = tempfile.TemporaryDirectory(prefix='nats-publication-')
        release_marker = Path(temporary.name) / 'release'
        handoff_marker = Path(temporary.name) / 'handoff'
        subject = 'userver.stop-fault.' + marker
        stream = 'USERVER_STOP_' + marker.upper()
        fault_subject = subject if scenario=='publisher' else '$JS.API.CONSUMER.MSG.NEXT.'+stream+'.reader'
        listener = socket.create_server(('127.0.0.1', 0))
        listener.settimeout(.2)
        fault, stopping = threading.Event(), threading.Event()
        threads, connections = [], []
        count = [0]

        def connection(client):
            upstream = socket.create_connection((args.broker_host, args.broker_port), timeout=3)
            upstream.settimeout(None)
            connections.extend([client, upstream])
            def replies():
                try:
                    while data := upstream.recv(65536):
                        if fault.is_set():
                            deadline = time.monotonic() + 5
                            while not release_marker.exists() and not stopping.is_set():
                                if time.monotonic() > deadline: raise TimeoutError('receipt release missing')
                                time.sleep(.005)
                        client.sendall(data)
                except OSError:
                    pass
            thread = threading.Thread(target=replies, daemon=True)
            threads.append(thread); thread.start()
            try:
                with client.makefile('rb') as incoming:
                    while line := incoming.readline():
                        parts = line.split()
                        body = b''
                        if parts and parts[0] in (b'PUB', b'HPUB'):
                            body = incoming.read(int(parts[-1])+2)
                        trigger = len(parts)>1 and parts[0] in (b'PUB',b'HPUB') and parts[1]==fault_subject.encode()
                        if trigger:
                            count[0] += 1
                            fault.set() # before forwarding; no receipt can race through
                            handoff_marker.write_text("native handoff")
                        upstream.sendall(line+body)
                        if trigger and mode == 'disconnect':
                            break
            except OSError:
                pass
            finally:
                for sock in (client,upstream):
                    try:sock.shutdown(socket.SHUT_RDWR)
                    except OSError:pass
                    sock.close()

        def accept():
            while not stopping.is_set():
                try:client,_=listener.accept()
                except socket.timeout:continue
                except OSError:return
                if fault.is_set():client.close();continue
                thread=threading.Thread(target=connection,args=(client,),daemon=True)
                threads.append(thread);thread.start()

        thread=threading.Thread(target=accept,daemon=True);thread.start()
        try:
            env={**os.environ,'SERVICELIB_NATS_FAULT_URL':f'nats://127.0.0.1:{listener.getsockname()[1]}',
                 'SERVICELIB_NATS_HANDOFF_MARKER':str(handoff_marker), 'SERVICELIB_NATS_RELEASE_MARKER':str(release_marker), 'SERVICELIB_NATS_FAULT_SUBJECT':subject,'SERVICELIB_NATS_FAULT_STREAM':stream}
            test='Nats.PublicationLateCompletionAfterCancellation'
            try:
                result=subprocess.run([args.binary,'--gtest_filter='+test],env=env,
                                      capture_output=True,text=True,timeout=12)
            except subprocess.TimeoutExpired as error:
                raise RuntimeError(f"mode={mode} fault={fault.is_set()} frames={count[0]}\n"+(error.stdout or b'').decode(errors='replace')+(error.stderr or b'').decode(errors='replace')) from error
            if result.returncode:raise RuntimeError(result.stdout+result.stderr)
            assert fault.is_set() and (count[0]==1 if scenario=='publisher' else count[0]>=1), 'native fault was not exercised'
            print('PASS publication native JetStream: cancel after handoff, retained metric owner, late PubAck counted once')
        finally:
            stopping.set();listener.close()
            for sock in connections:
                try:sock.shutdown(socket.SHUT_RDWR)
                except OSError:pass
                sock.close()
            thread.join(1)
            for worker in threads:worker.join(1)
            # Minimal NATS request to delete only this test stream, after the proxy closes.
            with socket.create_connection((args.broker_host,args.broker_port),timeout=3) as admin:
                admin.settimeout(3)
                inbox='_INBOX.'+uuid.uuid4().hex
                admin.sendall(('CONNECT {"verbose":false}\r\nSUB '+inbox+' 1\r\nPUB $JS.API.STREAM.DELETE.'+stream+' '+inbox+' 2\r\n{}\r\nPING\r\n').encode())
                with admin.makefile('rb') as response:
                    while line:=response.readline():
                        if line.startswith(b'PING'):admin.sendall(b'PONG\r\n')
                        if line.startswith(b'MSG '):
                            payload=response.read(int(line.split()[-1])+2)
                            assert json.loads(payload)['success'], 'test stream cleanup failed'
                            break


if __name__=='__main__': main()

