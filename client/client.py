import argparse
import asyncio
import logging

from protocol.basic import send_msg, recv_msg

log = logging.getLogger("client")
BAD_GATEWAY = (b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 27\r\n"
               b"Connection: close\r\n\r\nLocal server not reachable\n")

REGISTER_ID = 0  # reserved request_id meaning "this is a registration message, not a real request"


async def forward_to_local(host, port, request: bytes) -> bytes:
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except OSError:
        return BAD_GATEWAY
    writer.write(request)
    await writer.drain()
    response = await reader.read()
    writer.close()
    return response


async def handle_one_request(write_lock, tunnel_writer, request_id, request, local_host, local_port):
    log.info("request id=%d: %s", request_id, request.split(b"\r\n", 1)[0].decode(errors="replace"))
    response = await forward_to_local(local_host, local_port, request)
    async with write_lock:
        await send_msg(tunnel_writer, request_id, response)
    log.info("request id=%d: responded, %d bytes", request_id, len(response))


async def main(args):
    reader, writer = await asyncio.open_connection(args.relay_host, args.relay_port)
    log.info("connected to relay %s:%d", args.relay_host, args.relay_port)

    # Registration handshake: tell the relay which subdomain we want to be.
    await send_msg(writer, REGISTER_ID, args.subdomain.encode())
    log.info("registered as subdomain %r", args.subdomain)

    write_lock = asyncio.Lock()
    while True:
        try:
            request_id, request = await recv_msg(reader)
        except asyncio.IncompleteReadError:
            log.info("relay closed the tunnel")
            return
        asyncio.create_task(
            handle_one_request(write_lock, writer, request_id, request, args.local_host, args.local_port)
        )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--relay-host", default="127.0.0.1")
    p.add_argument("--relay-port", type=int, default=9000)
    p.add_argument("--local-host", default="127.0.0.1")
    p.add_argument("--local-port", type=int, default=3000)
    p.add_argument("--subdomain", required=True, help="e.g. 'abc' for abc.tunnelit.local")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    asyncio.run(main(p.parse_args()))