import asyncio

async def handle(reader, writer):
    head = await reader.readuntil(b"\r\n\r\n")
    request_line = head.split(b"\r\n", 1)[0]
    path = request_line.split(b" ")[1]
    delay = 3 if path == b"/slow" else 0
    await asyncio.sleep(delay)
    body = f"done ({path.decode()}, waited {delay}s)\n".encode()
    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s"
                 % (len(body), body))
    await writer.drain()
    writer.close()

async def main():
    srv = await asyncio.start_server(handle, "127.0.0.1", 3000)
    print("slow server listening on :3000")
    async with srv:
        await srv.serve_forever()

asyncio.run(main())