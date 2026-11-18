"""Actual adapter TLS/mode acceptance with isolated TLS terminators + real broker.

Terminators forward traffic, never fabricate Pulsar/admin responses. All resource
policy mutations are confined to the unique test namespace. Run in build image.
"""

import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import urllib.request
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pulsar_suitability"))
from pulsar_proxy import PulsarProxy


@asynccontextmanager
async def admin_tls(host, context):
    tasks, writers = set(), set()
    accepted = []

    async def forward(reader, writer, identity=None):
        while data := await reader.read(65536):
            if identity is not None:
                accepted.append(identity[0])
                identity = None
            writer.write(data)
            await writer.drain()

    async def connection(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        writers.add(writer)
        pair = []
        upstream = None
        try:
            remote, upstream = await asyncio.open_connection(host, 18080)
            writers.add(upstream)
            pair = [
                asyncio.create_task(
                    forward(reader, upstream, [writer.get_extra_info("peercert")])
                ),
                asyncio.create_task(forward(remote, writer)),
            ]
            done, _ = await asyncio.wait(pair, return_when=asyncio.FIRST_COMPLETED)
            for item in done:
                item.result()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            for item in pair:
                item.cancel()
            await asyncio.gather(*pair, return_exceptions=True)
            for stream in (writer, upstream):
                if stream:
                    stream.close()
                    writers.discard(stream)
            tasks.discard(task)

    server = await asyncio.start_server(connection, "127.0.0.1", 0, ssl=context)
    try:
        yield f"https://localhost:{server.sockets[0].getsockname()[1]}", accepted
    finally:
        server.close()
        await server.wait_closed()
        for writer in tuple(writers):
            writer.close()
        for task in tuple(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def certificates(root):
    def openssl(*args):
        subprocess.run(
            ["openssl", *map(str, args)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

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
    for name, ca, host, usage in [
        ("server", "ca", "localhost", "serverAuth"),
        ("wrong-host", "ca", "wrong.invalid", "serverAuth"),
        ("untrusted", "rogue", "localhost", "serverAuth"),
        ("client", "ca", "client", "clientAuth"),
        ("outsider", "rogue", "outsider", "clientAuth"),
    ]:
        openssl(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            "/CN=" + host,
            "-keyout",
            root / (name + ".key"),
            "-out",
            root / (name + ".csr"),
        )
        ext = root / (name + ".ext")
        ext.write_text(f"subjectAltName=DNS:{host}\nextendedKeyUsage={usage}\n")
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
            ext,
            "-out",
            root / (name + ".pem"),
        )


def server_context(root, name="server", mtls=False):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(root / (name + ".pem"), root / (name + ".key"))
    if mtls:
        ctx.load_verify_locations(root / "ca.pem")
        ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


async def main():
    host = os.environ.get("PULSAR_HOST", "host.docker.internal")
    ns = "public/userver-tls-modes-" + uuid.uuid4().hex
    topics = []
    results = []

    def request(method, path, body=None):
        req = urllib.request.Request(
            f"http://{host}:18080" + path,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            data = response.read()
            return json.loads(data) if data else None

    def topic(label):
        topics.append(label)
        # Explicit fixture provisioning permits post-failure broker inspection.
        request("PUT", f"/admin/v2/persistent/{ns}/{label}")
        request(
            "PUT",
            f"/admin/v2/persistent/{ns}/{label}/subscription/workers",
            {"ledgerId": -1, "entryId": -1},
        )
        return f"persistent://{ns}/{label}"

    async def run(label, values, success=True, mode=None):
        env = {k: v for k, v in os.environ.items() if not k.startswith("PULSAR_TLS_")}
        env.update(PULSAR_TEST_TOPIC=topic(label), **values)
        process = await asyncio.create_subprocess_exec(
            sys.argv[1],
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            output, _ = await asyncio.wait_for(process.communicate(), 18)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise AssertionError(f"{label}: adapter process exceeded 18s")
        text = output.decode(errors="replace")
        assert (
            process.returncode == 0 and "PASS Pulsar SDK native acceptance" in text
        ), (label, text[-6000:])
        stats = request("GET", f"/admin/v2/persistent/{ns}/{label}/stats")
        sub = stats["subscriptions"]["workers"]
        assert sub["msgBacklog"] == 0 and sub["unackedMessages"] == 0, (label, sub)
        if mode:
            assert sub["type"] == mode, (label, sub["type"])
        if not success:
            assert stats["msgInCounter"] == 0, "failed TLS attempt published data"
        results.append(label)
        print("PASS " + label + " (adapter + broker stats)", flush=True)

    request("PUT", "/admin/v2/namespaces/" + ns, {})
    try:
        for policy, field in [
            ("autoTopicCreation", "allowAutoTopicCreation"),
            ("autoSubscriptionCreation", "allowAutoSubscriptionCreation"),
        ]:
            request("POST", f"/admin/v2/namespaces/{ns}/{policy}", {field: False})
        async with PulsarProxy(f"pulsar://{host}:16650") as proxy:
            for mode in ("Shared", "Exclusive", "Failover"):
                await run(
                    mode.lower(),
                    dict(
                        PULSAR_TEST_URL=proxy.url,
                        PULSAR_TEST_ADMIN=f"http://{host}:18080",
                        PULSAR_TEST_MODE="subscriptions",
                        PULSAR_SUBSCRIPTION_MODE=mode,
                    ),
                    mode=mode,
                )
        with tempfile.TemporaryDirectory(prefix="userver-adapter-tls-") as directory:
            root = Path(directory)
            certificates(root)
            # Fail each plane independently; the other uses a trusted hostname.
            cases = [
                ("trusted-ca", "server", "server", False, False, None, True),
                ("trusted-client", "server", "server", True, True, "client", True),
            ]
            for plane in ("native", "admin"):
                for cert in ("untrusted", "wrong-host"):
                    cases.append(
                        (
                            plane + "-" + cert,
                            cert if plane == "native" else "server",
                            cert if plane == "admin" else "server",
                            False,
                            False,
                            None,
                            False,
                        )
                    )
                for client in (None, "outsider"):
                    cases.append(
                        (
                            plane + "-client-" + (client or "missing"),
                            "server",
                            "server",
                            plane == "native",
                            plane == "admin",
                            client,
                            False,
                        )
                    )
            for (
                label,
                native_cert,
                admin_cert,
                native_mtls,
                admin_mtls,
                client,
                success,
            ) in cases:
                async with PulsarProxy(
                    f"pulsar://{host}:16650",
                    server_context(root, native_cert, native_mtls),
                ) as proxy:
                    async with admin_tls(
                        host, server_context(root, admin_cert, admin_mtls)
                    ) as (url, accepted):
                        values = dict(
                            PULSAR_TEST_URL=proxy.url,
                            PULSAR_TEST_ADMIN=url,
                            PULSAR_TEST_MODE="tls",
                            PULSAR_TLS_CA=str(root / "ca.pem"),
                            PULSAR_TLS_EXPECT="accept" if success else "reject",
                        )
                        if client:
                            values.update(
                                PULSAR_TLS_CERT=str(root / (client + ".pem")),
                                PULSAR_TLS_KEY=str(root / (client + ".key")),
                            )
                        await run(label, values, success)
                        if success:
                            assert proxy.sends == 1 and accepted, (
                                "positive did not traverse both TLS planes"
                            )
                            if admin_mtls:
                                assert all(accepted), (
                                    "HTTPS did not receive client identity"
                                )
                        else:
                            assert proxy.sends == 0, (
                                "TLS rejection bypassed native transport"
                            )
                            if label.startswith("native-"):
                                assert accepted, (
                                    "native negative failed early in admin preflight"
                                )
                            else:
                                assert not accepted, (
                                    "admin negative forwarded HTTP request bytes"
                                )
        print(
            f"PASS {len(results)} adapter TLS/subscription cases; shared broker untouched",
            flush=True,
        )
    finally:
        for name in topics:
            request("DELETE", f"/admin/v2/persistent/{ns}/{name}?force=true")
        request("DELETE", "/admin/v2/namespaces/" + ns)


asyncio.run(main())
