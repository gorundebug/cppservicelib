"""Drop exactly the first native async pull; allow official reconnect to recover.
Runs in the build container sharing the unchanged acceptance broker namespace.
"""
import os
import socket
import subprocess
import sys
import threading

listener = socket.create_server(('127.0.0.1', 0))
listener.settimeout(.2)
stopping = threading.Event()
dropped = threading.Event()
lock = threading.Lock()
connections = []
threads = []


def connection(client):
    upstream = socket.create_connection(('127.0.0.1', 4222), timeout=3)
    upstream.settimeout(None)
    with lock:
        connections.extend((client, upstream))

    def replies():
        try:
            while data := upstream.recv(65536):
                client.sendall(data)
        except OSError:
            pass

    thread = threading.Thread(target=replies, daemon=True)
    threads.append(thread)
    thread.start()
    try:
        with client.makefile('rb') as incoming:
            while line := incoming.readline():
                parts = line.split()
                body = b''
                if parts and parts[0] in (b'PUB', b'HPUB'):
                    body = incoming.read(int(parts[-1]) + 2)
                if len(parts) > 1 and parts[1].startswith(b'$JS.API.CONSUMER.MSG.NEXT.'):
                    with lock:
                        if not dropped.is_set():
                            dropped.set()
                            break
                upstream.sendall(line + body)
    except OSError:
        pass
    finally:
        for sock in (client, upstream):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()


def accept():
    while not stopping.is_set():
        try:
            client, _ = listener.accept()
        except socket.timeout:
            continue
        except OSError:
            return
        thread = threading.Thread(target=connection, args=(client,), daemon=True)
        threads.append(thread)
        thread.start()


acceptor = threading.Thread(target=accept)
acceptor.start()
try:
    env = dict(os.environ, SERVICELIB_NATS_TEST_URL='nats://127.0.0.1:4222',
               SERVICELIB_NATS_RECONNECT_URL=f'nats://127.0.0.1:{listener.getsockname()[1]}')
    result = subprocess.run([sys.argv[1], '--gtest_filter=Nats.AsyncPullProductionReconnect'],
                            env=env, timeout=30)
    if result.returncode or not dropped.is_set():
        raise SystemExit(result.returncode or 1)
    print('PASS first-pull disconnect, official reconnect and production rearm', flush=True)
finally:
    stopping.set()
    listener.close()
    for sock in connections:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()
    acceptor.join(2)
    for thread in threads:
        thread.join(2)
