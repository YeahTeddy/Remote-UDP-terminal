import os
import re
import shlex
import struct

MAGIC = 0x5554
TYPE_COMMAND = 0x01
TYPE_OUTPUT = 0x02
TYPE_ACK = 0x03
TYPE_HEARTBEAT = 0x04
TYPE_INTERRUPT = 0x05
TYPE_STDIN = 0x06
TYPE_RESIZE = 0x07
TYPE_WINDOW_UPDATE = 0x08

VALID_TYPES = {
    TYPE_COMMAND,
    TYPE_OUTPUT,
    TYPE_ACK,
    TYPE_HEARTBEAT,
    TYPE_INTERRUPT,
    TYPE_STDIN,
    TYPE_RESIZE,
    TYPE_WINDOW_UPDATE,
}
HEADER_FORMAT = '>H B I H I'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAX_DATA_SIZE = 1472 - HEADER_SIZE
MAX_SEQUENCE = 0xFFFFFFFF
DATA_SEQUENCE_MOD = MAX_SEQUENCE
HEARTBEAT_SEQ = MAX_SEQUENCE
OUTPUT_WINDOW_SIZE = 8
ACK_TIMEOUT = 0.5
MAX_RETRIES = 5
RECV_BUFFER_LIMIT_PACKETS = 32
RECV_BUFFER_LIMIT_BYTES = MAX_DATA_SIZE * RECV_BUFFER_LIMIT_PACKETS
FLOW_CONTROL_IDLE_TIMEOUT = 5
OUTPUT_DONE_PREFIX = b'\x00CWD:'
PROMPT_INFO_PREFIX = b'\x00PROMPT:'
WINDOW_UPDATE_PREFIX = b'\x00WIN:'
RESIZE_PREFIX = b'\x00SZ:'
ANSI_ESCAPE_RE = re.compile(
    rb'\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))'
)
PTY_PREFIX = 'pty'
PTY_COMMANDS = {'top', 'vim', 'vi', 'nano', 'less'}


def next_data_seq(seq: int) -> int:
    return (seq + 1) % DATA_SEQUENCE_MOD


def is_sequence_ahead(seq: int, expected: int) -> bool:
    distance = (seq - expected) % DATA_SEQUENCE_MOD
    return 0 < distance < DATA_SEQUENCE_MOD // 2


def strip_ansi_sequences(data: bytes) -> bytes:
    return ANSI_ESCAPE_RE.sub(b'', data)


def normalize_command_input(text: str) -> str:
    text = strip_ansi_sequences(text.encode('utf-8', errors='replace')).decode('utf-8', errors='replace')
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    result = []
    for ch in text:
        if ch in ('\b', '\x7f'):
            if result:
                result.pop()
        else:
            result.append(ch)
    return ''.join(result)


def _split_command(cmd: str) -> list[str]:
    try:
        return shlex.split(cmd, posix=os.name != 'nt')
    except ValueError:
        return cmd.split()


def strip_pty_prefix(cmd: str) -> str:
    tokens = _split_command(cmd)
    if tokens and tokens[0].lower() == PTY_PREFIX:
        return cmd.strip()[len(PTY_PREFIX):].lstrip()
    return cmd


def should_use_pty_command(cmd: str) -> bool:
    tokens = _split_command(cmd)
    if not tokens:
        return False
    first = tokens[0].lower()
    if first == PTY_PREFIX:
        return len(tokens) > 1
    exe = os.path.basename(first).lower()
    if exe.endswith('.exe'):
        exe = exe[:-4]
    return exe in PTY_COMMANDS


def pack_output_done(cwd: str) -> bytes:
    return OUTPUT_DONE_PREFIX + cwd.encode('utf-8')


def unpack_output_done(data: bytes) -> str | None:
    if data == b'':
        return ''
    if data.startswith(OUTPUT_DONE_PREFIX):
        return data[len(OUTPUT_DONE_PREFIX):].decode('utf-8', errors='replace')
    return None


def pack_prompt_info(user: str, host: str, cwd: str, server_session_id: str = '') -> bytes:
    return PROMPT_INFO_PREFIX + '\0'.join((user, host, cwd, server_session_id)).encode('utf-8')


def unpack_prompt_info(data: bytes) -> tuple[str, str, str, str] | None:
    if not data.startswith(PROMPT_INFO_PREFIX):
        return None
    parts = data[len(PROMPT_INFO_PREFIX):].decode('utf-8', errors='replace').split('\0', 3)
    if len(parts) == 3:
        parts.append('')
    if len(parts) != 4:
        return None
    return (parts[0], parts[1], parts[2], parts[3])


def pack_window_update(available_packets: int, available_bytes: int) -> bytes:
    if not 0 <= available_packets <= 0xFFFF:
        raise ValueError("Available packet window out of range")
    if not 0 <= available_bytes <= 0xFFFFFFFF:
        raise ValueError("Available byte window out of range")
    return WINDOW_UPDATE_PREFIX + struct.pack('>H I', available_packets, available_bytes)


def unpack_window_update(data: bytes) -> tuple[int, int] | None:
    if not data.startswith(WINDOW_UPDATE_PREFIX):
        return None
    payload = data[len(WINDOW_UPDATE_PREFIX):]
    if len(payload) != struct.calcsize('>H I'):
        return None
    return struct.unpack('>H I', payload)


def pack_resize(rows: int, cols: int) -> bytes:
    if not 1 <= rows <= 1000:
        raise ValueError("Terminal rows out of range")
    if not 1 <= cols <= 1000:
        raise ValueError("Terminal columns out of range")
    return RESIZE_PREFIX + struct.pack('>H H', rows, cols)


def unpack_resize(data: bytes) -> tuple[int, int] | None:
    if not data.startswith(RESIZE_PREFIX):
        return None
    payload = data[len(RESIZE_PREFIX):]
    if len(payload) != struct.calcsize('>H H'):
        return None
    rows, cols = struct.unpack('>H H', payload)
    if not 1 <= rows <= 1000 or not 1 <= cols <= 1000:
        return None
    return rows, cols


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
