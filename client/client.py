import argparse
import asyncio
import logging

from protocol.basic import send_msg, recv_msg

log = logging.getLogger("client")
BAD_GATEWAY = (b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 27\r\n"
               b"Connection: close\r\n\r\nLocal server not reachable\n")

REGISTER_ID = 0
HEARTBEAT_ID = 0xFFFFFFFF          # reserved id for heartbeat pings
HEARTBEAT_INTERVAL = 15            # seconds between pings
CONNECT_TIMEOUT = 10               # seconds to wait for the initial TCP connect
MAX_BACKOFF = 30                   # cap for reconnect backoff (seconds)


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
    try:
        async with write_lock:
            await send_msg(tunnel_writer, request_id, response)
    except (ConnectionError, OSError):
        log.warning("request id=%d: tunnel died before response could be sent", request_id)
        return
    log.info("request id=%d: responded, %d bytes", request_id, len(response))


async def heartbeat_loop(write_lock, writer):
    """Periodically pings the relay so a silently-dead connection
    (e.g. a WiFi drop with no clean TCP close) gets detected
    proactively instead of only when a real request happens to fail."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            async with write_lock:
                await send_msg(writer, HEARTBEAT_ID, b"")
        except (ConnectionError, OSError):
            return  # connection is dead; the main loop's recv_msg will notice too


async def run_one_session(args) -> None:
    """One full connect -> register -> serve session. Raises on any
    failure so main()'s reconnect loop can catch it and retry."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(args.relay_host, args.relay_port), timeout=CONNECT_TIMEOUT
    )
    log.info("connected to relay %s:%d", args.relay_host, args.relay_port)

    registration = f"{args.token}:{args.subdomain}".encode()
    await send_msg(writer, REGISTER_ID, registration)
    _, ack = await recv_msg(reader)
    if ack != b"OK":
        log.error("registration rejected: %s", ack.decode(errors="replace"))
        writer.close()
        return  # auth failures don't fix themselves — don't retry

    log.info("registered as subdomain %r", args.subdomain)

    write_lock = asyncio.Lock()
    heartbeat_task = asyncio.create_task(heartbeat_loop(write_lock, writer))
    try:
        while True:
            request_id, request = await recv_msg(reader)
            if request_id == HEARTBEAT_ID:
                continue
            asyncio.create_task(
                handle_one_request(write_lock, writer, request_id, request, args.local_host, args.local_port)
            )
    finally:
        heartbeat_task.cancel()
        writer.close()


async def main(args):
    backoff = 1
    while True:
        session_start = asyncio.get_event_loop().time()
        try:
            await run_one_session(args)
            return  # only returns normally after an auth rejection
        except (ConnectionError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError) as e:
            ran_for = asyncio.get_event_loop().time() - session_start
            if ran_for > 60:
                backoff = 1  # session was healthy a while — don't punish it for one blip
            log.warning("session ended (%s), reconnecting in %ds", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--relay-host", default="127.0.0.1")
    p.add_argument("--relay-port", type=int, default=9000)
    p.add_argument("--local-host", default="127.0.0.1")
    p.add_argument("--local-port", type=int, default=3000)
    p.add_argument("--subdomain", required=True)
    p.add_argument("--token", required=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    asyncio.run(main(p.parse_args()))