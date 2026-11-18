"""Host-side isolated oversized-record gate; never reconfigures a shared broker."""
import pathlib
import subprocess
import tempfile
import time
import uuid

root = pathlib.Path(__file__).resolve().parents[1]
name = 'sa-nats-large-' + uuid.uuid4().hex[:12]
runner = name + '-test'

def run(*args, **kwargs):
    return subprocess.run(['docker', *args], check=True, timeout=60, **kwargs)

with tempfile.TemporaryDirectory(prefix='sa-nats-large-') as temp:
    config = pathlib.Path(temp) / 'nats.conf'
    config.write_text('port: 4222\nmax_payload: 16MB\njetstream { store_dir: "/tmp/js" }\n')
    try:
        run('run', '-d', '--name', name, '--network', 'none',
            '-v', f'{config}:/etc/nats/nats.conf:ro', 'nats:2.15.3-alpine',
            '-c', '/etc/nats/nats.conf')
        for _ in range(100):
            result = run('logs', name, capture_output=True, text=True)
            if 'Server is ready' in result.stdout + result.stderr:
                break
            time.sleep(.1)
        else:
            raise RuntimeError('owned broker startup timeout')
        run('run', '--rm', '--name', runner, '--network', 'container:' + name,
            '-v', f'{root}:/workspace',
            '-v', 'cppservicelib_cppservicelib-conan2:/conan:ro',
            '-e', 'SERVICELIB_NATS_LARGE_TEST_URL=nats://127.0.0.1:4222',
            '--entrypoint', 'timeout', 'cppservicelib-build:latest', '30s',
            '/workspace/build-nats/debug/servicelib_nats_test',
            '--gtest_filter=Nats.AsyncPullProductionOversize')
    finally:
        subprocess.run(['docker', 'rm', '-f', runner, name], capture_output=True, timeout=20)
