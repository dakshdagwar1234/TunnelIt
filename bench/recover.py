import asyncio
import sys
import time

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
HOST_HEADER = sys.argv[3] if len(sys.argv) > 3 else "bench.tunnelit.local"


async def probe():
    try:
        r, w = await asyncio.wait_for(asyncio.open_connection(HOST, PORT), 1)
        w.write(f"GET / HTTP/1.1\r\nHost: {HOST_HEADER}\r\n"
                f"Connection: close\r\n\r\n".encode())
        await w.drain()
        data = await asyncio.wait_for(r.read(), 2)
        w.close()
        return data.startswith(b"HTTP/1.1 200") or data.startswith(b"HTTP/1.0 200")
    except Exception:
        return False


async def main():
    down_at = None
    print("polling... kill the tunnel/relay now (Ctrl+C here to stop)")
    while True:
        ok = await probe()
        now = time.perf_counter()
        if not ok and down_at is None:
            down_at = now
            print("DOWN detected")
        elif ok and down_at is not None:
            print(f"RECOVERED after {now - down_at:.1f}s")
            down_at = None
        await asyncio.sleep(0.1)


asyncio.run(main())