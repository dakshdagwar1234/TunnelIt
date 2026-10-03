import argparse
import asyncio
import statistics
import time


async def one_request(host, port, host_header, path):
    t0 = time.perf_counter()
    status = 0
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=10)
        writer.write(
            f"GET {path} HTTP/1.1\r\nHost: {host_header}\r\n"
            f"Connection: close\r\n\r\n".encode())
        await writer.drain()
        data = await asyncio.wait_for(reader.read(), timeout=15)
        writer.close()
        if data.startswith(b"HTTP/"):
            status = int(data.split(b" ", 2)[1])
    except Exception:
        pass
    return time.perf_counter() - t0, status


def summarize(label, results, wall):
    lat = sorted(r[0] * 1000 for r in results)
    ok = sum(1 for r in results if r[1] == 200)
    n = len(results)

    def pct(p):
        return lat[min(n - 1, int(n * p))]

    print(f"\n== {label} ==")
    print(f"requests: {n}   ok: {ok}   errors: {n - ok}")
    print(f"total wall time: {wall:.2f}s   throughput: {n / wall:.1f} req/s")
    print(f"latency ms  median: {statistics.median(lat):.1f}   "
          f"p95: {pct(0.95):.1f}   p99: {pct(0.99):.1f}   max: {lat[-1]:.1f}")


async def sequential(args, n):
    results = []
    t0 = time.perf_counter()
    for _ in range(n):
        results.append(await one_request(
            args.host, args.port, args.host_header, args.path))
    return results, time.perf_counter() - t0


async def concurrent(args, n):
    t0 = time.perf_counter()
    results = await asyncio.gather(*[
        one_request(args.host, args.port, args.host_header, args.path)
        for _ in range(n)])
    return list(results), time.perf_counter() - t0


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--host-header", default="bench.tunnelit.local")
    p.add_argument("--path", default="/")
    p.add_argument("--seq", type=int, default=20, help="sequential requests")
    p.add_argument("--conc", type=int, default=50, help="concurrent requests")
    p.add_argument("--rounds", type=int, default=3, help="repeat each test")
    args = p.parse_args()

    await one_request(args.host, args.port, args.host_header, args.path)  # warm-up

    for r in range(1, args.rounds + 1):
        res, wall = await sequential(args, args.seq)
        summarize(f"round {r}: sequential x{args.seq}", res, wall)
        res, wall = await concurrent(args, args.conc)
        summarize(f"round {r}: concurrent x{args.conc}", res, wall)


asyncio.run(main())