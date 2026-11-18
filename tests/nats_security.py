"""Isolated official NATS auth/TLS and stock userver startup suitability gate.

Run on the Docker host. --inside is an internal startup-proxy runner only.
Never changes/restarts the shared acceptance fixture.
"""

import argparse
import asyncio
import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path


async def startup_proxy(binary, mode):
    tasks, writers = set(), set()
    connections = 0

    async def copy(reader, writer):
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()

    async def connected(reader, writer):
        nonlocal connections
        connections += 1
        task = asyncio.current_task()
        tasks.add(task)
        writers.add(writer)
        upstream = None
        pumps = []
        try:
            # Delay before the real server INFO/TLS handshake. The deadline case
            # never receives INFO; cancellation cases complete a genuine connect.
            if mode == "tls-deadline":
                remote, upstream = await asyncio.open_connection("127.0.0.1", 4222)
                writer.write(await remote.readline())
                await writer.drain()
                await asyncio.sleep(2)
            else:
                await asyncio.sleep(2 if mode == "deadline" else 0.18)
                remote, upstream = await asyncio.open_connection("127.0.0.1", 4222)
            writers.add(upstream)
            pumps = [
                asyncio.create_task(copy(reader, upstream)),
                asyncio.create_task(copy(remote, writer)),
            ]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            for pump in pumps:
                pump.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            for stream in (writer, upstream):
                if stream:
                    stream.close()
                    writers.discard(stream)
            tasks.discard(task)

    server = await asyncio.start_server(connected, "127.0.0.1", 0)
    try:
        env = dict(
            os.environ,
            NATS_SECURITY_URL=f"{'tls' if mode == 'tls-deadline' else 'nats'}://127.0.0.1:{server.sockets[0].getsockname()[1]}",
            NATS_SECURITY_START=mode,
        )
        process = await asyncio.create_subprocess_exec(
            binary,
            "--gtest_filter=NatsSecurity.StartupResponsiveness",
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            output, _ = await asyncio.wait_for(process.communicate(), 8)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise
        print(output.decode(), end="")
        assert process.returncode == 0, "startup test failed"
        assert connections == (0 if mode == "pre-cancel" else 1), connections
    finally:
        server.close()
        await server.wait_closed()
        for writer in tuple(writers):
            writer.close()
        for task in tuple(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inside",
        choices=["deadline", "tls-deadline", "cancel", "task-cancel", "pre-cancel"],
    )
    parser.add_argument(
        "--binary", default="/workspace/build-nats/debug/servicelib_nats_test"
    )
    parser.add_argument("--artifacts", required=False)
    parser.add_argument("--only-startup", action="store_true")
    args = parser.parse_args()
    if args.inside:
        asyncio.run(startup_proxy(args.binary, args.inside))
        return
    sdk = Path(__file__).resolve().parents[1]
    artifacts = Path(
        args.artifacts or tempfile.mkdtemp(prefix="nats-userver-security-")
    ).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    image = os.environ.get("NATS_SECURITY_SERVER_IMAGE", "nats:2.15.3-alpine")
    build_image = os.environ.get(
        "NATS_SECURITY_BUILD_IMAGE", "cppservicelib-build:latest"
    )
    results = []

    def run(*command, timeout=15):
        return subprocess.run(
            command, capture_output=True, text=True, check=True, timeout=timeout
        )

    with tempfile.TemporaryDirectory(prefix="nats-userver-identities-") as temp:
        root = Path(temp)

        def openssl(*command):
            run("openssl", *map(str, command))

        for ca in ("ca", "rogue"):
            openssl(
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=" + ca,
                "-keyout",
                root / (ca + ".key"),
                "-out",
                root / (ca + ".pem"),
            )
        for name, ca, usage in [
            ("server", "ca", "serverAuth"),
            ("client", "ca", "clientAuth"),
            ("rogue-client", "rogue", "clientAuth"),
        ]:
            openssl(
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                "/CN=" + name,
                "-keyout",
                root / (name + ".key"),
                "-out",
                root / (name + ".csr"),
            )
            extension = root / (name + ".ext")
            extension.write_text(
                "subjectAltName=DNS:localhost\nextendedKeyUsage=" + usage + "\n"
            )
            openssl(
                "x509",
                "-req",
                "-in",
                root / (name + ".csr"),
                "-CA",
                root / (ca + ".pem"),
                "-CAkey",
                root / (ca + ".key"),
                "-CAcreateserial",
                "-days",
                "1",
                "-extfile",
                extension,
                "-out",
                root / (name + ".pem"),
            )
        (root / "malformed.creds").write_text("invalid-test-credential\n")
        if not args.only_startup:
            helper = root / "identity-tool"
            helper.mkdir()
            (helper / "go.mod").write_text(
                "module nats-security-identities\n\ngo 1.25.0\n\nrequire (\n github.com/nats-io/jwt/v2 v2.8.2\n github.com/nats-io/nkeys v0.4.16\n)\n"
            )
            (helper / "main.go").write_text(
                (sdk / "tests/nats_security_credentials.go").read_text()
            )
            subprocess.run(
                [os.environ.get("GO", "go"), "run", "-mod=mod", ".", str(root)],
                cwd=helper,
                env={**os.environ, "GOWORK": "off"},
                capture_output=True,
                check=True,
                timeout=120,
            )
        # Keys are readable by the isolated server's non-root UID; ephemeral dir
        # is removed at exit and never copied into retained evidence.
        for file in root.iterdir():
            if file.is_file():
                file.chmod(0o644)
        fixtures = {
            "password": 'authorization { users: [{user: "adapter", password: "test-secret-password"}, {user: "scoped", password: "test-secret-password", permissions: {publish: ["allowed.>"], subscribe: ["allowed.>"]}}] }',
            "token": 'authorization { token: "test-secret-token" }',
            "tls": 'tls { cert_file: "/fixture/server.pem", key_file: "/fixture/server.key" }',
            "mtls": 'tls { cert_file: "/fixture/server.pem", key_file: "/fixture/server.key", ca_file: "/fixture/ca.pem", verify: true }',
        }
        if not args.only_startup:
            fixtures["jwt"] = (root / "jwt.conf").read_text()
        for fixture, security in fixtures.items():
            if args.only_startup and fixture != "password":
                continue
            (root / (fixture + ".conf")).write_text(
                'port: 4222\nhttp_port: 8222\njetstream { store_dir: "/tmp/js" }\n'
                + security
                + "\n"
            )
            container = "userver-nats-security-" + uuid.uuid4().hex
            try:
                run(
                    "docker",
                    "run",
                    "--detach",
                    "--name",
                    container,
                    "-v",
                    str(root) + ":/fixture:ro",
                    image,
                    "-c",
                    "/fixture/" + fixture + ".conf",
                )
                # The NATS monitor is readiness only; application protocol uses SDK.
                for _ in range(50):
                    ready = subprocess.run(
                        [
                            "docker",
                            "exec",
                            container,
                            "wget",
                            "-qO-",
                            "http://127.0.0.1:8222/healthz",
                        ],
                        capture_output=True,
                        check=False,
                    )
                    if ready.returncode == 0:
                        break
                    time.sleep(0.1)
                else:
                    raise RuntimeError(
                        "isolated broker readiness failed: "
                        + run("docker", "logs", container).stderr
                    )
                base = {"NATS_SECURITY_URL": "nats://127.0.0.1:4222"}
                if fixture == "password":
                    base.update(
                        NATS_SECURITY_USER="adapter",
                        NATS_SECURITY_PASSWORD="test-secret-password",
                    )
                if fixture == "jwt":
                    base["NATS_SECURITY_CREDS"] = "/fixture/valid.creds"
                if fixture == "token":
                    base["NATS_SECURITY_TOKEN"] = "test-secret-token"
                if fixture in ("tls", "mtls"):
                    base.update(
                        NATS_SECURITY_URL="tls://localhost:4222",
                        NATS_SECURITY_CA="/fixture/ca.pem",
                    )
                if fixture == "mtls":
                    base.update(
                        NATS_SECURITY_CERT="/fixture/client.pem",
                        NATS_SECURITY_KEY="/fixture/client.key",
                    )
                cases = [("core", {}), ("jetstream", {"NATS_SECURITY_JS": "1"})]
                if fixture == "password":
                    cases += [
                        (
                            "wrong-password",
                            {
                                "NATS_SECURITY_PASSWORD": "wrong-secret",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                        (
                            "missing-password",
                            {
                                "NATS_SECURITY_USER": "",
                                "NATS_SECURITY_PASSWORD": "",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                        (
                            "permission-denied",
                            {
                                "NATS_SECURITY_USER": "scoped",
                                "NATS_SECURITY_DENIED": "1",
                            },
                        ),
                        (
                            "missing-creds",
                            {
                                "NATS_SECURITY_USER": "",
                                "NATS_SECURITY_PASSWORD": "",
                                "NATS_SECURITY_CREDS": "/fixture/missing.creds",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                        (
                            "malformed-creds",
                            {
                                "NATS_SECURITY_USER": "",
                                "NATS_SECURITY_PASSWORD": "",
                                "NATS_SECURITY_CREDS": "/fixture/malformed.creds",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                    ]
                if fixture == "token":
                    cases += [
                        (
                            "wrong-token",
                            {
                                "NATS_SECURITY_TOKEN": "wrong-token-secret",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                        (
                            "missing-token",
                            {"NATS_SECURITY_TOKEN": "", "NATS_SECURITY_NEGATIVE": "1"},
                        ),
                    ]
                if fixture in ("tls", "mtls"):
                    cases += [
                        (
                            "hostname-override",
                            {
                                "NATS_SECURITY_URL": "tls://127.0.0.1:4222",
                                "NATS_SECURITY_HOSTNAME": "localhost",
                            },
                        ),
                        (
                            "hostname-mismatch",
                            {
                                "NATS_SECURITY_HOSTNAME": "wrong.invalid",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                        (
                            "url-hostname-mismatch",
                            {
                                "NATS_SECURITY_URL": "tls://127.0.0.1:4222",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                        (
                            "untrusted-ca",
                            {
                                "NATS_SECURITY_CA": "/fixture/rogue.pem",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                        (
                            "missing-ca",
                            {"NATS_SECURITY_CA": "", "NATS_SECURITY_NEGATIVE": "1"},
                        ),
                    ]
                if fixture == "mtls":
                    cases += [
                        (
                            "missing-client",
                            {
                                "NATS_SECURITY_CERT": "",
                                "NATS_SECURITY_KEY": "",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                        (
                            "untrusted-client",
                            {
                                "NATS_SECURITY_CERT": "/fixture/rogue-client.pem",
                                "NATS_SECURITY_KEY": "/fixture/rogue-client.key",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                        (
                            "mismatched-key",
                            {
                                "NATS_SECURITY_KEY": "/fixture/rogue-client.key",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        ),
                    ]
                if fixture == "jwt":
                    cases += [
                        (
                            name,
                            {
                                "NATS_SECURITY_CREDS": "/fixture/" + name + ".creds",
                                "NATS_SECURITY_NEGATIVE": "1",
                            },
                        )
                        for name in ("untrusted-issuer", "expired", "wrong-seed")
                    ]
                if fixture == "tls":
                    cases += [("tls-deadline", {"NATS_SECURITY_HOSTNAME": "localhost"})]
                if args.only_startup:
                    cases = []
                if fixture == "password":
                    cases += [
                        (mode, {})
                        for mode in ("pre-cancel", "deadline", "cancel", "task-cancel")
                    ]
                for name, changes in cases:
                    env = {**base, **changes}
                    command = [
                        "docker",
                        "run",
                        "--rm",
                        "--network",
                        "container:" + container,
                        "-v",
                        str(sdk) + ":/workspace:ro",
                        "-v",
                        "cppservicelib_cppservicelib-conan2:/conan:ro",
                        "-v",
                        str(root) + ":/fixture:ro",
                    ]
                    for key in env:
                        command += [
                            "-e",
                            key,
                        ]  # values never appear in argv or diagnostics
                    if name in (
                        "pre-cancel",
                        "deadline",
                        "tls-deadline",
                        "cancel",
                        "task-cancel",
                    ):
                        command += [
                            build_image,
                            "python3",
                            "/workspace/tests/nats_security.py",
                            "--inside",
                            name,
                            "--binary",
                            args.binary,
                        ]
                    else:
                        command += [
                            build_image,
                            args.binary,
                            "--gtest_filter=NatsSecurity.NativeMatrix",
                        ]
                    start = time.monotonic()
                    process = subprocess.run(
                        command,
                        env={**os.environ, **env},
                        capture_output=True,
                        text=True,
                        timeout=20,
                        check=False,
                    )
                    output = process.stdout + process.stderr
                    for secret in (
                        "test-secret-password",
                        "test-secret-token",
                        "wrong-secret",
                        "wrong-token-secret",
                    ):
                        assert secret not in output, "test leaked auth value"
                    (artifacts / (fixture + "-" + name + ".log")).write_text(output)
                    assert process.returncode == 0, (
                        f"{fixture}/{name} failed; see {artifacts}"
                    )
                    results.append(
                        {
                            "case": fixture + "/" + name,
                            "seconds": time.monotonic() - start,
                        }
                    )
                    print("PASS " + fixture + "/" + name, flush=True)
                    (artifacts / "results.json").write_text(
                        json.dumps(results, indent=2) + "\n"
                    )
            finally:
                subprocess.run(
                    ["docker", "rm", "-f", container],
                    capture_output=True,
                    timeout=10,
                    check=False,
                )
    print(
        f"PASS: {len(results)} isolated native security/startup cases; evidence {artifacts}"
    )


if __name__ == "__main__":
    main()
