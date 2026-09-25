import argparse
import asyncio
import logging

from protocol.basic import send_msg, recv_msg

log = logging.getLogger("client")
BAD_GATEWAY = (b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 27\r\n"
               b"Connection: close\r\n\r\nLocal server not reachable\n")


async def forward_to_local(host, port, request: bytes) -> bytes:
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except OSError:
        return BAD_GATEWAY
    writer.write(request)
    await writer.drain()
    response = await reader.read()        # read until EOF (Connection: close)
    writer.close()
    return response


async def main(args):
    reader, writer = await asyncio.open_connection(args.relay_host, args.relay_port)
    log.info("connected to relay %s:%d", args.relay_host, args.relay_port)
    while True:
        try:
            request = await recv_msg(reader)
        except asyncio.IncompleteReadError:
            log.info("relay closed the tunnel")
            return
        log.info("request: %s", request.split(b"\r\n", 1)[0].decode(errors="replace"))
        response = await forward_to_local(args.local_host, args.local_port, request)
        await send_msg(writer, response)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--relay-host", default="127.0.0.1")
    p.add_argument("--relay-port", type=int, default=9000)
    p.add_argument("--local-host", default="127.0.0.1")
    p.add_argument("--local-port", type=int, default=3000)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    asyncio.run(main(p.parse_args()))