import asyncio
import logging

from protocol.basic import send_msg, recv_msg

TUNNEL_PORT = 9000
PUBLIC_PORT = 8080
REASONS = {400: "Bad Request", 502: "Bad Gateway"}
REQUEST_TIMEOUT = 30  # seconds to wait for a response before giving up

log = logging.getLogger("relay")


class Tunnel:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer
        self.closed = asyncio.Event()
        self.pending: dict[int, asyncio.Future] = {}  # request_id -> Future waiting for its reply
        self.write_lock = asyncio.Lock()               # only guards the moment of writing
        self._next_id = 0

    def next_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def close(self):
        if self.closed.is_set():
            return
        self.writer.close()
        self.closed.set()
        # nobody left waiting should hang forever if the tunnel dies
        for fut in self.pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError("tunnel closed"))
        self.pending.clear()


async def respond(writer, status: int, text: str) -> None:
    body = text.encode()
    writer.write(
        f"HTTP/1.1 {status} {REASONS[status]}\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body
    )
    await writer.drain()
    writer.close()


async def read_http_request(reader):
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head[:-4].split(b"\r\n")
    request_line, kept, content_length = lines[0], [], 0
    for line in lines[1:]:
        name, _, value = line.partition(b":")
        name = name.strip().lower()
        if name == b"content-length":
            content_length = int(value)
        if name in (b"connection", b"proxy-connection", b"keep-alive"):
            continue
        kept.append(line)
    kept.append(b"Connection: close")
    body = await reader.readexactly(content_length) if content_length else b""
    request = b"\r\n".join([request_line] + kept) + b"\r\n\r\n" + body
    return request, request_line.decode(errors="replace")


class Relay:
    def __init__(self):
        self.tunnel: Tunnel | None = None

    async def handle_tunnel(self, reader, writer):
        """One coroutine per tunnel connection. This is now the ONLY
        place that reads from the tunnel — it runs for the tunnel's
        entire lifetime, continuously pulling responses off the wire
        and routing each one to whoever is waiting for that request_id."""
        peer = writer.get_extra_info("peername")
        if self.tunnel:
            log.info("new client replaces old tunnel")
            self.tunnel.close()
        tunnel = self.tunnel = Tunnel(reader, writer)
        log.info("tunnel client registered from %s", peer)

        try:
            while True:
                request_id, payload = await recv_msg(reader)
                fut = tunnel.pending.pop(request_id, None)
                if fut is None:
                    log.warning("response for unknown/expired request_id=%d", request_id)
                    continue
                if not fut.done():
                    fut.set_result(payload)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            tunnel.close()
            if self.tunnel is tunnel:
                self.tunnel = None
            log.info("tunnel from %s closed", peer)

    async def handle_public(self, reader, writer):
        try:
            request, request_line = await read_http_request(reader)
        except Exception as e:
            log.warning("bad public request: %r", e)
            await respond(writer, 400, "Bad request")
            return

        tunnel = self.tunnel
        if tunnel is None:
            await respond(writer, 502, "No tunnel client connected")
            return

        request_id = tunnel.next_id()
        fut = asyncio.get_running_loop().create_future()
        tunnel.pending[request_id] = fut

        try:
            async with tunnel.write_lock:          # only the write itself is serialized
                await send_msg(tunnel.writer, request_id, request)
            response = await asyncio.wait_for(fut, timeout=REQUEST_TIMEOUT)
        except asyncio.TimeoutError:
            tunnel.pending.pop(request_id, None)
            await respond(writer, 502, "Tunnel request timed out")
            return
        except (ConnectionError, OSError):
            await respond(writer, 502, "Tunnel connection lost")
            return

        log.info("%s (id=%d) -> %d bytes back", request_line, request_id, len(response))
        writer.write(response)
        await writer.drain()
        writer.close()


async def main():
    relay = Relay()
    tunnel_srv = await asyncio.start_server(relay.handle_tunnel, "0.0.0.0", TUNNEL_PORT)
    public_srv = await asyncio.start_server(relay.handle_public, "0.0.0.0", PUBLIC_PORT)
    log.info("tunnel port %d, public port %d", TUNNEL_PORT, PUBLIC_PORT)
    async with tunnel_srv, public_srv:
        await asyncio.gather(tunnel_srv.serve_forever(), public_srv.serve_forever())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    asyncio.run(main())