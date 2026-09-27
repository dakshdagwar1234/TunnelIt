import asyncio
import logging

from protocol.basic import send_msg, recv_msg
from relay.tokens import ALLOWED_TOKENS

TUNNEL_PORT = 9000
PUBLIC_PORT = 8080
REASONS = {400: "Bad Request", 502: "Bad Gateway"}
REQUEST_TIMEOUT = 30
REGISTER_ID = 0
HEARTBEAT_ID = 0xFFFFFFFF

log = logging.getLogger("relay")


class Tunnel:
    def __init__(self, subdomain, reader, writer):
        self.subdomain = subdomain
        self.reader, self.writer = reader, writer
        self.closed = asyncio.Event()
        self.pending: dict[int, asyncio.Future] = {}
        self.write_lock = asyncio.Lock()
        self._next_id = 0

    def next_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def close(self):
        if self.closed.is_set():
            return
        self.writer.close()
        self.closed.set()
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
    host_header = None
    for line in lines[1:]:
        name, _, value = line.partition(b":")
        name = name.strip().lower()
        value = value.strip()
        if name == b"content-length":
            content_length = int(value)
        if name == b"host":
            host_header = value.decode(errors="replace")
        if name in (b"connection", b"proxy-connection", b"keep-alive"):
            continue
        kept.append(line)
    kept.append(b"Connection: close")
    body = await reader.readexactly(content_length) if content_length else b""
    request = b"\r\n".join([request_line] + kept) + b"\r\n\r\n" + body
    return request, request_line.decode(errors="replace"), host_header


def extract_subdomain(host_header: str | None) -> str | None:
    if not host_header:
        return None
    hostname = host_header.split(":")[0]
    parts = hostname.split(".")
    if len(parts) < 2:
        return None
    return parts[0]


class Relay:
    def __init__(self):
        self.tunnels: dict[str, Tunnel] = {}

    async def handle_tunnel(self, reader, writer):
        peer = writer.get_extra_info("peername")

        try:
            request_id, payload = await recv_msg(reader)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            writer.close()
            return

        if request_id != REGISTER_ID:
            log.warning("client %s skipped registration, closing", peer)
            writer.close()
            return

        try:
            token, subdomain = payload.decode(errors="replace").split(":", 1)
        except ValueError:
            log.warning("client %s sent malformed registration, closing", peer)
            writer.close()
            return
        subdomain = subdomain.strip()

        if token not in ALLOWED_TOKENS:
            log.warning("client %s failed auth (bad token), closing", peer)
            await send_msg(writer, REGISTER_ID, b"AUTH_FAILED")
            writer.close()
            return
        if not subdomain:
            await send_msg(writer, REGISTER_ID, b"EMPTY_SUBDOMAIN")
            writer.close()
            return

        if subdomain in self.tunnels:
            log.info("subdomain %r reconnecting, replacing old tunnel", subdomain)
            self.tunnels[subdomain].close()

        tunnel = Tunnel(subdomain, reader, writer)
        self.tunnels[subdomain] = tunnel
        await send_msg(writer, REGISTER_ID, b"OK")
        log.info("tunnel registered: subdomain=%r from %s", subdomain, peer)

        try:
            while True:
                request_id, payload = await recv_msg(reader)
                if request_id == HEARTBEAT_ID:
                    continue
                fut = tunnel.pending.pop(request_id, None)
                if fut is None:
                    log.warning("response for unknown/expired request_id=%d on %r", request_id, subdomain)
                    continue
                if not fut.done():
                    fut.set_result(payload)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            tunnel.close()
            if self.tunnels.get(subdomain) is tunnel:
                del self.tunnels[subdomain]
            log.info("tunnel closed: subdomain=%r from %s", subdomain, peer)

    async def handle_public(self, reader, writer):
        try:
            request, request_line, host_header = await read_http_request(reader)
        except Exception as e:
            log.warning("bad public request: %r", e)
            await respond(writer, 400, "Bad request")
            return

        subdomain = extract_subdomain(host_header)
        if subdomain is None:
            await respond(writer, 400, f"Missing or invalid Host header: {host_header!r}")
            return

        tunnel = self.tunnels.get(subdomain)
        if tunnel is None:
            await respond(writer, 502, f"No tunnel registered for subdomain {subdomain!r}")
            return

        request_id = tunnel.next_id()
        fut = asyncio.get_running_loop().create_future()
        tunnel.pending[request_id] = fut

        try:
            async with tunnel.write_lock:
                await send_msg(tunnel.writer, request_id, request)
            response = await asyncio.wait_for(fut, timeout=REQUEST_TIMEOUT)
        except asyncio.TimeoutError:
            tunnel.pending.pop(request_id, None)
            await respond(writer, 502, "Tunnel request timed out")
            return
        except (ConnectionError, OSError):
            await respond(writer, 502, "Tunnel connection lost")
            return

        log.info("%s (subdomain=%s, id=%d) -> %d bytes back", request_line, subdomain, request_id, len(response))
        writer.write(response)
        await writer.drain()
        writer.close()


async def main():
    relay = Relay()
    tunnel_srv = await asyncio.start_server(relay.handle_tunnel, "0.0.0.0", TUNNEL_PORT, reuse_address=True)
    public_srv = await asyncio.start_server(relay.handle_public, "0.0.0.0", PUBLIC_PORT, reuse_address=True)
    log.info("tunnel port %d, public port %d", TUNNEL_PORT, PUBLIC_PORT)
    async with tunnel_srv, public_srv:
        await asyncio.gather(tunnel_srv.serve_forever(), public_srv.serve_forever())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    asyncio.run(main())