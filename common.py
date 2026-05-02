import struct

MAGIC = 0x1234
TYPE_DATA = 0
TYPE_ACK = 1
TYPE_HEARTBEAT = 2
TYPE_INTERRUPT = 3

HEADER_FORMAT = '>H B B I I'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAX_DATA_SIZE = 1472 - HEADER_SIZE

def pack_msg(msg_type: int, seq: int, client_id: int, data: bytes) -> bytes:
    if len(data) > MAX_DATA_SIZE:
        raise ValueError(f"Data size exceeds maximum of {MAX_DATA_SIZE} bytes")
    header = struct.pack(HEADER_FORMAT, MAGIC, msg_type, seq, client_id, len(data))
    return header + data

def unpack_msg(raw_data: bytes) -> tuple[int, int, int, bytes] | None:
    if len(raw_data) < HEADER_SIZE:
        return None
    magic, msg_type, seq, client_id, data_len = struct.unpack(HEADER_FORMAT, raw_data[:HEADER_SIZE])
    if magic != MAGIC:
        return None
    if len(raw_data) < HEADER_SIZE + data_len:
        return None
    data = raw_data[HEADER_SIZE:HEADER_SIZE + data_len]
    return (msg_type, seq, client_id, data)
