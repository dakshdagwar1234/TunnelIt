import asyncio
import sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 3001
DELAY = float(sys.argv[2]) if len(sys.argv) > 2 else 0.2
RESPONSE = (b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n"
            b"Connection: close\r\n\r\nok\n")


async def handle(reader, writer):
    try:
        await reader.readuntil(b"\r\n\r\n")
        await asyncio.sleep(DELAY)
        writer.write(RESPONSE)
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


async def main():
    server = await asyncio.start_server(handle, "127.0.0.1", PORT)
    print(f"slow server on :{PORT}, delay {DELAY}s")
    async with server:
        await server.serve_forever()


asyncio.run(main())