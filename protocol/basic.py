import struct

HEADER = struct.Struct("!I")          # 4 bytes, big-endian unsigned int
MAX_MSG = 16 * 1024 * 1024            # refuse absurd lengths

async def send_msg(writer, payload: bytes) -> None:
    writer.write(HEADER.pack(len(payload)) + payload)
    await writer.drain()

async def recv_msg(reader) -> bytes:
    header = await reader.readexactly(HEADER.size)
    (length,) = HEADER.unpack(header)
    if length > MAX_MSG:
        raise ValueError(f"message too large: {length}")
    return await reader.readexactly(length)