"""Run SDK checks against the shared broker with a test-owned namespace only."""

import asyncio
import json
import os
from pathlib import Path
import sys
import urllib.request
import urllib.error
import uuid
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pulsar_suitability"))
from pulsar_proxy import PulsarProxy


async def main():
    host = os.getenv("PULSAR_HOST", "host.docker.internal")
    admin = f"http://{host}:18080"
    ns = "public/userver-sdk-" + uuid.uuid4().hex

    def request(method, path, data=None):
        req = urllib.request.Request(
            admin + path,
            method=method,
            data=None if data is None else json.dumps(data).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if method == "DELETE" and error.code == 404:
                return None
            raise

    request("PUT", "/admin/v2/namespaces/" + ns, {})
    try:
        for policy, field in [
            ("autoTopicCreation", "allowAutoTopicCreation"),
            ("autoSubscriptionCreation", "allowAutoSubscriptionCreation"),
        ]:
            request("POST", f"/admin/v2/namespaces/{ns}/{policy}", {field: False})
        for mode in os.getenv("PULSAR_TEST_MODES", "basic,fault").split(","):
            async with PulsarProxy(f"pulsar://{host}:16650") as proxy:
                proxy.drop_receipts = mode == "fault"
                temporary = tempfile.TemporaryDirectory(prefix="pulsar-publication-")
                marker_root = Path(temporary.name)
                async def receipt_gate():
                    (marker_root / "handoff").write_text("held")
                    async with asyncio.timeout(5):
                        while not (marker_root / "release").exists():
                            await asyncio.sleep(.005)
                if mode == "publication-cancel":
                    proxy.receipt_gate = receipt_gate
                    proxy.receipt_forwarded = lambda: (marker_root / "delivered").write_text("forwarded")

                env = dict(
                    os.environ,
                    PULSAR_TEST_URL=proxy.url,
                    PULSAR_TEST_ADMIN=admin,
                    PULSAR_TEST_TOPIC=f"persistent://{ns}/events",
                    PULSAR_TEST_MODE=mode,
                    PULSAR_HANDOFF_MARKER=str(marker_root / "handoff"),
                    PULSAR_RELEASE_MARKER=str(marker_root / "release"),
                    PULSAR_DELIVERED_MARKER=str(marker_root / "delivered"),
                )
                process = await asyncio.create_subprocess_exec(
                    sys.argv[1],
                    env=env,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                try:
                    output, _ = await asyncio.wait_for(process.communicate(), 45)
                except TimeoutError:
                    process.kill()
                    await process.wait()
                    raise AssertionError("SDK process exceeded 45s")
                text = output.decode(errors="replace")
                print(
                    "\n".join(
                        line
                        for line in text.splitlines()
                        if line.startswith(("PASS", "FAIL"))
                    ),
                    flush=True,
                )
                if process.returncode:
                    print(text[-5000:])
                assert process.returncode == 0, process.returncode
                temporary.cleanup()
                if mode == "publication-cancel":
                    assert proxy.sends == 1, proxy.sends
                if mode == "fault":
                    assert proxy.sends == 4, proxy.sends

    finally:
        request("DELETE", f"/admin/v2/persistent/{ns}/events?force=true")
        request("DELETE", f"/admin/v2/namespaces/{ns}")


asyncio.run(main())
