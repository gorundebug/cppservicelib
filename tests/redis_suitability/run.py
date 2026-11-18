"""Native probe supervisor. RESP proxy only injects a lost real-broker reply.
No application transport implementation, reactor, or adapter bridge is supplied.
Run in build image; broker remains untouched except a unique test stream.
"""
import asyncio
import json
import os
from pathlib import Path
import signal
import ssl
import tempfile
import sys
import time
import uuid

async def frame(reader):
    line = await reader.readuntil(b'\r\n')
    tag, value = line[:1], line[1:-2]
    if tag == b'*':
        children = [await frame(reader) for _ in range(max(0, int(value)))]
        return line + b''.join(c[0] for c in children), [c[1] for c in children]
    if tag == b'$' and int(value) >= 0:
        data = await reader.readexactly(int(value) + 2)
        return line + data, data[:-2]
    return line, value

async def main():
    host = os.environ.get('REDIS_HOST', 'host.docker.internal')
    port = int(os.environ.get('REDIS_PORT', '16379'))
    password = os.environ.get('REDIS_PASSWORD', 'adapter-test')
    xadds = 0
    async def proxy(reader, writer):
        nonlocal xadds
        upstream = None
        try:
            source, upstream = await asyncio.open_connection(host, port)
            while True:
                raw, args = await frame(reader)
                upstream.write(raw)
                await upstream.drain()
                response, _ = await frame(source)
                if isinstance(args, list) and args[0].upper() == b'XADD':
                    xadds += 1
                    if xadds == 1:
                        break  # Redis executed it, but client receives no reply.
                writer.write(response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            if upstream:
                upstream.close()
    server = await asyncio.start_server(proxy, '127.0.0.1', 18099)
    cert_dir = tempfile.TemporaryDirectory(prefix='userver-redis-tls-')
    cert = Path(cert_dir.name) / 'cert.pem'
    private_key = Path(cert_dir.name) / 'key.pem'
    openssl = await asyncio.create_subprocess_exec('openssl','req','-x509','-newkey','rsa:2048','-nodes',
        '-keyout',str(private_key),'-out',str(cert),'-days','1','-subj','/CN=wrong.example',
        '-addext','subjectAltName=DNS:wrong.example', stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    assert await openssl.wait() == 0
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls_context.load_cert_chain(cert, private_key)
    async def tls_proxy(reader, writer):
        upstream_reader, upstream_writer = await asyncio.open_connection(host, port)
        async def copy(source, destination):
            try:
                while data := await source.read(65536):
                    destination.write(data)
                    await destination.drain()
            finally:
                destination.close()
        await asyncio.gather(copy(reader, upstream_writer), copy(upstream_reader, writer), return_exceptions=True)
    tls_server = await asyncio.start_server(tls_proxy, '127.0.0.1', 18100, ssl=tls_context)
    verifier = ssl.create_default_context(cafile=str(cert))
    try:
        await asyncio.open_connection('127.0.0.1',18100,ssl=verifier)
    except ssl.SSLCertVerificationError as error:
        assert 'IP address mismatch' in str(error) or 'Hostname mismatch' in str(error)
        print('PASS fixture control: trusted certificate rejected for wrong hostname')
    else:
        raise AssertionError('TLS fixture did not enforce hostname verification')
    key = 'userver-redis-probe-' + uuid.uuid4().hex
    direct = dict(password=password, sentinels=[dict(host=host, port=port)], shards=[dict(name='standalone')])
    fault = dict(direct, sentinels=[dict(host='127.0.0.1', port=18099)])
    tls = dict(direct, sentinels=[dict(host='127.0.0.1',port=18100)], secure_connection=True)
    env = dict(os.environ, SSL_CERT_FILE=str(cert), PROBE_KEY=key,
               SECDIST_CONFIG=json.dumps(dict(redis_settings=dict(direct=direct, fault=fault, tls=tls))))
    log_path = Path('/tmp/redis-suitability-native.log')
    with log_path.open('wb') as log:
        process = await asyncio.create_subprocess_exec(sys.argv[1], '--config', str(Path(__file__).with_name('config.yaml')), env=env, stdout=log, stderr=log)
        try:
            for _ in range(150):
                if process.returncode is not None:
                    raise RuntimeError('probe startup failed; see ' + str(log_path))
                try:
                    reader, writer = await asyncio.open_connection('127.0.0.1', 18098)
                    break
                except ConnectionError:
                    await asyncio.sleep(.1)
            else:
                raise TimeoutError('probe startup timed out')
            writer.write(b'POST /probe HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            await writer.drain()
            result = await asyncio.wait_for(reader.read(), 15)
            writer.close()
            print(result.decode(errors='replace'))
            assert b'PASS binary isolation cancellation no-replay' in result, 'native probe failed'
            assert xadds == 1, f'{xadds} XADD attempts: ambiguous replay'
            reader, writer = await asyncio.open_connection('127.0.0.1',18098)
            writer.write(b'POST /probe?tls=1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            await writer.drain()
            tls_result = await asyncio.wait_for(reader.read(),5)
            writer.close()
            print(tls_result.decode(errors='replace'))
            hostname_gap = b'UNSAFE:' in tls_result
            print('TLS_HOSTNAME_GATE=' + ('FAIL' if hostname_gap else 'REJECTED'))
            start = time.monotonic()
            process.send_signal(signal.SIGTERM)
            await asyncio.wait_for(process.wait(), 3)
            assert process.returncode == 0
            print(f'PASS SIGTERM component teardown {time.monotonic()-start:.3f}s; XADD attempts={xadds}')
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
            tls_server.close()
            await tls_server.wait_closed()
            cert_dir.cleanup()
            server.close()
            await server.wait_closed()
            # Delete only our unique key, including on failed probes.
            reader, writer = await asyncio.open_connection(host, port)
            for args in [[b'AUTH', password.encode()], [b'DEL', key.encode()]]:
                writer.write(b'*' + str(len(args)).encode() + b'\r\n' + b''.join(b'$'+str(len(a)).encode()+b'\r\n'+a+b'\r\n' for a in args))
                await writer.drain()
                await frame(reader)
            writer.close()
    assert not hostname_gap, 'userver TLS accepted wrong hostname: complete profile ineligible'

asyncio.run(main())
