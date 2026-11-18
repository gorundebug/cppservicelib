"""Test-only Pulsar frame proxy: forwards real broker traffic, rewrites lookup address.
No broker responses are fabricated. Can withhold native SEND_RECEIPT for ambiguity.
"""

import asyncio
from urllib.parse import urlsplit


def varint(n):
    data = bytearray()
    while n > 127:
        data.append((n & 127) | 128)
        n >>= 7
    data.append(n)
    return bytes(data)


def number(data, index):
    n = shift = 0
    while True:
        v = data[index]
        index += 1
        n |= (v & 127) << shift
        if v < 128:
            return n, index
        shift += 7


def fields(data):
    i = 0
    while i < len(data):
        tag, i = number(data, i)
        wire = tag & 7
        if wire == 0:
            value, i = number(data, i)
        elif wire == 2:
            length, i = number(data, i)
            value = data[i : i + length]
            i += length
        elif wire in (1, 5):
            length = 8 if wire == 1 else 4
            value = data[i : i + length]
            i += length
        else:
            raise ValueError("unsupported protobuf wire")
        yield tag >> 3, wire, value


def encode(items):
    result = b""
    for key, wire, value in items:
        result += varint(key << 3 | wire)
        result += (
            varint(value)
            if wire == 0
            else ((varint(len(value)) if wire == 2 else b"") + value)
        )
    return result


class PulsarProxy:
    def __init__(self, url, ssl=None):
        self.remote = urlsplit(url)
        self.ssl = ssl
        self.tasks = set()
        self.writers = set()
        self.sends = 0
        self.drop_receipts = False

    async def __aenter__(self):
        self.server = await asyncio.start_server(
            self.accept, "127.0.0.1", 0, ssl=self.ssl
        )
        port = self.server.sockets[0].getsockname()[1]
        self.url = (
            ("pulsar+ssl" if self.ssl else "pulsar") + "://localhost:" + str(port)
        )
        return self

    async def __aexit__(self, *args):
        self.server.close()
        await self.server.wait_closed()
        for writer in tuple(self.writers):
            writer.close()
        for task in tuple(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def accept(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        self.writers.add(writer)
        upstream = None
        try:
            source, upstream = await asyncio.open_connection(
                self.remote.hostname, self.remote.port or 6650
            )
            self.writers.add(upstream)

            async def forward(r, w, response):
                while True:
                    length = int.from_bytes(await r.readexactly(4), "big")
                    if length > 16 << 20:
                        raise ValueError("frame exceeds fixture bound")
                    frame = await r.readexactly(length)
                    command_length = int.from_bytes(frame[:4], "big")
                    command = list(fields(frame[4 : 4 + command_length]))
                    kind = next(v for k, t, v in command if k == 1)
                    if not response and kind == 6:
                        self.sends += 1
                    if response and kind == 7 and self.drop_receipts:
                        continue
                    if response and kind == 24:
                        rewritten = []
                        for k, t, v in command:
                            if k == 24:
                                nested = [
                                    (nk, nt, nv)
                                    for nk, nt, nv in fields(v)
                                    if nk not in (1, 2, 8)
                                ]
                                nested.extend(
                                    [
                                        (1, 2, self.url.encode()),
                                        (2, 2, self.url.encode()),
                                        (8, 0, 1),
                                    ]
                                )
                                v = encode(nested)
                            rewritten.append((k, t, v))
                        raw = encode(rewritten)
                        frame = (
                            len(raw).to_bytes(4, "big")
                            + raw
                            + frame[4 + command_length :]
                        )
                    w.write(len(frame).to_bytes(4, "big") + frame)
                    await w.drain()

            tasks = [
                asyncio.create_task(forward(reader, upstream, False)),
                asyncio.create_task(forward(source, writer, True)),
            ]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for item in done:
                    item.result()
            finally:
                for item in tasks:
                    item.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            self.writers.discard(writer)
            if upstream:
                upstream.close()
                self.writers.discard(upstream)
            self.tasks.discard(task)
