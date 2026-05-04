import struct

MAGIC = 0x5554
TYPE_COMMAND = 0x01
TYPE_OUTPUT = 0x02
TYPE_ACK = 0x03
TYPE_HEARTBEAT = 0x04
TYPE_INTERRUPT = 0x05

VALID_TYPES = {TYPE_COMMAND, TYPE_OUTPUT, TYPE_ACK, TYPE_HEARTBEAT, TYPE_INTERRUPT}
HEADER_FORMAT = '>H B I H I'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAX_DATA_SIZE = 1472 - HEADER_SIZE
MAX_SEQUENCE = 0xFFFFFFFF
DATA_SEQUENCE_MOD = MAX_SEQUENCE
HEARTBEAT_SEQ = MAX_SEQUENCE
OUTPUT_DONE_PREFIX = b'\x00CWD:'
PROMPT_INFO_PREFIX = b'\x00PROMPT:'


def next_data_seq(seq: int) -> int:
    return (seq + 1) % DATA_SEQUENCE_MOD


def normalize_command_input(text: str) -> str:
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    result = []
    for ch in text:
        if ch in ('\b', '\x7f'):
            if result:
                result.pop()
        else:
            result.append(ch)
    return ''.join(result)


def pack_output_done(cwd: str) -> bytes:
    return OUTPUT_DONE_PREFIX + cwd.encode('utf-8')


def unpack_output_done(data: bytes) -> str | None:
    if data == b'':
        return ''
    if data.startswith(OUTPUT_DONE_PREFIX):
        return data[len(OUTPUT_DONE_PREFIX):].decode('utf-8', errors='replace')
    return None


def pack_prompt_info(user: str, host: str, cwd: str) -> bytes:
    return PROMPT_INFO_PREFIX + '\0'.join((user, host, cwd)).encode('utf-8')


def unpack_prompt_info(data: bytes) -> tuple[str, str, str] | None:
    if not data.startswith(PROMPT_INFO_PREFIX):
        return None
    parts = data[len(PROMPT_INFO_PREFIX):].decode('utf-8', errors='replace').split('\0', 2)
    if len(parts) != 3:
        return None
    return (parts[0], parts[1], parts[2])


def pack_msg(msg_type: int, seq: int, client_id: int, data: bytes) -> bytes:
    if msg_type not in VALID_TYPES:
        raise ValueError("Invalid message type")
    if not 0 <= seq <= MAX_SEQUENCE:
        raise ValueError("Sequence number out of range")
    if not 0 <= client_id <= 0xFFFFFFFF:
        raise ValueError("Client ID out of range")
    if len(data) > MAX_DATA_SIZE:
        raise ValueError(f"Data size exceeds maximum of {MAX_DATA_SIZE} bytes")
    header = struct.pack(HEADER_FORMAT, MAGIC, msg_type, seq, len(data), client_id)
    return header + data


def unpack_msg(raw_data: bytes) -> tuple[int, int, int, bytes] | None:
    if len(raw_data) < HEADER_SIZE:
        return None
    magic, msg_type, seq, data_len, client_id = struct.unpack(HEADER_FORMAT, raw_data[:HEADER_SIZE])
    if magic != MAGIC or msg_type not in VALID_TYPES:
        return None
    if len(raw_data) != HEADER_SIZE + data_len:
        return None
    data = raw_data[HEADER_SIZE:]
    return (msg_type, seq, client_id, data)
