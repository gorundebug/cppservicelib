"""Container-side ACL driver. The private fixture manifest is never printed."""

import asyncio
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pulsar_suitability"))
from pulsar_proxy import PulsarProxy


async def main():
    manifest = json.loads(Path(sys.argv[2]).read_text())
    binary = urlsplit(manifest["url"]).port
    admin = urlsplit(manifest["adminUrl"]).port
    async with PulsarProxy(f"pulsar://host.docker.internal:{binary}") as proxy:
        env = dict(
            os.environ,
            PULSAR_TEST_URL=proxy.url,
            PULSAR_TEST_ADMIN=f"http://host.docker.internal:{admin}",
            PULSAR_TEST_TOPIC="persistent://acl/events/new-topic",
            PULSAR_TEST_MODE="auth",
            PULSAR_ADMIN_TOKEN=manifest["tokens"]["tenant-admin"],
            PULSAR_PRODUCER_TOKEN=manifest["tokens"]["producer"],
            PULSAR_CONSUMER_TOKEN=manifest["tokens"]["consumer"],
            PULSAR_OUTSIDER_TOKEN=manifest["tokens"]["outsider"],
            PULSAR_TOPIC_TOKEN=manifest["tokens"]["topic-producer"],
        )
        process = await asyncio.create_subprocess_exec(
            sys.argv[1],
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            output, _ = await asyncio.wait_for(process.communicate(), 30)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise AssertionError("ACL process stop unbounded")
        text = output.decode(errors="replace")
        assert not any(token in text for token in manifest["tokens"].values()), (
            "credential leaked"
        )
        print(
            "\n".join(
                line for line in text.splitlines() if line.startswith(("PASS", "FAIL"))
            )
        )
        assert process.returncode == 0, (
            "ACL SDK check failed (secret-safe output above)"
        )


asyncio.run(main())
