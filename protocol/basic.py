import struct

HEADER = struct.Struct("!II")   # length (4 bytes) + request_id (4 bytes), big-endian
MAX_PAYLOAD = 16 * 1024 * 1024  # 16 MB safety cap


def encode_msg(request_id: int, payload: bytes) -> bytes:
    
    if len(payload) > MAX_PAYLOAD:
        raise ValueError(f"payload too large: {len(payload)} bytes")
    return HEADER.pack(len(payload), request_id) + payload


async def send_msg(writer, request_id: int, payload: bytes) -> None:
    writer.write(encode_msg(request_id, payload))
    await writer.drain()


async def recv_msg(reader):
    
    header = await reader.readexactly(HEADER.size)
    length, request_id = HEADER.unpack(header)
    if length > MAX_PAYLOAD:
        raise ValueError(f"message too large: {length}")
    payload = await reader.readexactly(length)
    return request_id, payload


class Decoder:
    

    def __init__(self):
        self._buf = bytearray()

    def feed(self, chunk: bytes) -> None:
        self._buf.extend(chunk)

    def pop_ready(self):
        out = []
        while True:
            if len(self._buf) < HEADER.size:
                break
            length, request_id = HEADER.unpack(self._buf[: HEADER.size])
            total = HEADER.size + length
            if len(self._buf) < total:
                break
            payload = bytes(self._buf[HEADER.size:total])
            del self._buf[:total]
            out.append((request_id, payload))
        return out