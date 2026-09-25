import asyncio
import logging

from protocol.basic import send_msg, recv_msg

TUNNEL_PORT = 9000
PUBLIC_PORT = 8080
REASONS = {400: "Bad Request", 502: "Bad Gateway"}

log = logging.getLogger("relay")


class Tunnel:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer
        self.closed = asyncio.Event()

    def close(self):

        self.writer.close()
        self.closed.set()


async def respond(writer, status: int, text: str) -> None:
    body = text.encode()
    writer.write(
        f"HTTP/1.1 {status} {REASONS[status]}\r\n"
        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body
    )
    await writer.drain()
    writer.close()


async def read_http_request(reader):
    """Read one full HTTP request. Returns (bytes_to_forward, request_line)."""
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head[:-4].split(b"\r\n")
    request_line, kept, content_length = lines[0], [], 0
    for line in lines[1:]:
        name, _, value = line.partition(b":")
        name = name.strip().lower()
        if name == b"content-length":
            content_length = int(value)
        if name in (b"connection", b"proxy-connection", b"keep-alive"):
            continue                      # we force our own below
        kept.append(line)
    kept.append(b"Connection: close")     # local server must close after replying
    body = await reader.readexactly(content_length) if content_length else b""
    request = b"\r\n".join([request_line] + kept) + b"\r\n\r\n" + body
    return request, request_line.decode(errors="replace")


class Relay:
    def __init__(self):
        self.tunnel = None
        self.lock = asyncio.Lock()        # one request in the tunnel at a time

    async def handle_tunnel(self, reader, writer):
        peer = writer.get_extra_info("peername")
        if self.tunnel:
            log.info("new client replaces old tunnel")
            self.tunnel.close()
        tunnel = self.tunnel = Tunnel(reader, writer)
        log.info("tunnel client registered from %s", peer)
        await tunnel.closed.wait()        # keep handler alive; don't read here!
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

        async with self.lock:
            tunnel = self.tunnel
            if tunnel is None:
                await respond(writer, 502, "No tunnel client connected")
                return
            try:
                await send_msg(tunnel.writer, request_id=0, payload=request)
                _, response = await recv_msg(tunnel.reader)
            except (asyncio.IncompleteReadError, ConnectionError, OSError):
                tunnel.close()
                await respond(writer, 502, "Tunnel connection lost")
                return

        log.info("%s -> %d bytes back", request_line, len(response))
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