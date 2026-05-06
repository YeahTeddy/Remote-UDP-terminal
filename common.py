"""UDP 远程终端的共享协议定义和编解码工具。

本模块只放客户端和服务端都需要的常量与纯函数，避免两端对协议格式、
消息类型、序列号规则或控制载荷格式产生不一致。
"""

import os
import re
import shlex
import struct

# 数据包魔数用于快速识别本项目协议，避免把其他 UDP 数据误当成有效消息。
MAGIC = 0x5554

# 消息类型占 1 字节；客户端和服务端的分支处理依赖这些数值完全一致。
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

# 包头格式固定为: magic(2) + type(1) + seq(4) + data_len(2) + client_id(4)。
# 使用大端序保证不同系统之间打包结果一致。
HEADER_FORMAT = '>H B I H I'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# 1472 是常见以太网 MTU 1500 扣除 IPv4(20) 和 UDP(8) 头后的 UDP 载荷上限。
MAX_DATA_SIZE = 1472 - HEADER_SIZE

# 普通数据序列号使用 [0, MAX_SEQUENCE) 循环；MAX_SEQUENCE 自身保留给心跳包。
MAX_SEQUENCE = 0xFFFFFFFF
DATA_SEQUENCE_MOD = MAX_SEQUENCE
HEARTBEAT_SEQ = MAX_SEQUENCE

# 输出侧采用滑动窗口 + ACK 重传；这些参数控制吞吐、重试和接收端反压。
OUTPUT_WINDOW_SIZE = 8
ACK_TIMEOUT = 0.5
MAX_RETRIES = 5
RECV_BUFFER_LIMIT_PACKETS = 32
RECV_BUFFER_LIMIT_BYTES = MAX_DATA_SIZE * RECV_BUFFER_LIMIT_PACKETS
FLOW_CONTROL_IDLE_TIMEOUT = 5

# 控制载荷以 NUL 开头，区别于普通命令输出，便于复用 TYPE_OUTPUT/TYPE_ACK 通道。
OUTPUT_DONE_PREFIX = b'\x00CWD:'
PROMPT_INFO_PREFIX = b'\x00PROMPT:'
WINDOW_UPDATE_PREFIX = b'\x00WIN:'
RESIZE_PREFIX = b'\x00SZ:'

# 匹配常见 CSI/OSC ANSI 转义序列，用于把命令行输入或普通输出恢复成纯文本。
ANSI_ESCAPE_RE = re.compile(
    rb'\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))'
)

# pty 前缀显式要求交互式伪终端；下面的命令默认也需要 PTY 才能正常工作。
PTY_PREFIX = 'pty'
PTY_COMMANDS = {'top', 'vim', 'vi', 'nano', 'less'}


def next_data_seq(seq: int) -> int:
    """返回下一个普通数据序列号，并在保留心跳序列号前回绕。"""
    return (seq + 1) % DATA_SEQUENCE_MOD


def is_sequence_ahead(seq: int, expected: int) -> bool:
    """判断 seq 是否位于 expected 之后，用半个序列号空间处理回绕歧义。"""
    distance = (seq - expected) % DATA_SEQUENCE_MOD
    return 0 < distance < DATA_SEQUENCE_MOD // 2


def strip_ansi_sequences(data: bytes) -> bytes:
    """移除 ANSI 控制序列，保留用户可读的文本字节。"""
    return ANSI_ESCAPE_RE.sub(b'', data)


def normalize_command_input(text: str) -> str:
    """清洗交互式输入，把退格、回车和 ANSI 序列转换成服务端可执行命令。"""
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
    """按当前平台规则拆分命令；解析失败时退回空白分割以保持容错。"""
    try:
        return shlex.split(cmd, posix=os.name != 'nt')
    except ValueError:
        return cmd.split()


def strip_pty_prefix(cmd: str) -> str:
    """去掉显式 PTY 前缀，让服务端执行真正的命令内容。"""
    tokens = _split_command(cmd)
    if tokens and tokens[0].lower() == PTY_PREFIX:
        return cmd.strip()[len(PTY_PREFIX):].lstrip()
    return cmd


def should_use_pty_command(cmd: str) -> bool:
    """判断命令是否需要伪终端，以支持全屏程序和显式 pty 前缀。"""
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
    """构造命令输出结束标记，并携带服务端最新工作目录。"""
    return OUTPUT_DONE_PREFIX + cwd.encode('utf-8')


def unpack_output_done(data: bytes) -> str | None:
    """解析输出结束标记；返回 None 表示这只是普通输出。"""
    if data == b'':
        return ''
    if data.startswith(OUTPUT_DONE_PREFIX):
        return data[len(OUTPUT_DONE_PREFIX):].decode('utf-8', errors='replace')
    return None


def pack_prompt_info(user: str, host: str, cwd: str, server_session_id: str = '') -> bytes:
    """打包提示符信息，心跳 ACK 使用它让客户端显示远端用户、主机和目录。"""
    return PROMPT_INFO_PREFIX + '\0'.join((user, host, cwd, server_session_id)).encode('utf-8')


def unpack_prompt_info(data: bytes) -> tuple[str, str, str, str] | None:
    """解析提示符信息，并兼容旧格式中没有 server_session_id 的情况。"""
    if not data.startswith(PROMPT_INFO_PREFIX):
        return None
    parts = data[len(PROMPT_INFO_PREFIX):].decode('utf-8', errors='replace').split('\0', 3)
    if len(parts) == 3:
        parts.append('')
    if len(parts) != 4:
        return None
    return (parts[0], parts[1], parts[2], parts[3])


def pack_window_update(available_packets: int, available_bytes: int) -> bytes:
    """打包接收窗口大小，服务端据此限制未确认输出包数量。"""
    if not 0 <= available_packets <= 0xFFFF:
        raise ValueError("Available packet window out of range")
    if not 0 <= available_bytes <= 0xFFFFFFFF:
        raise ValueError("Available byte window out of range")
    return WINDOW_UPDATE_PREFIX + struct.pack('>H I', available_packets, available_bytes)


def unpack_window_update(data: bytes) -> tuple[int, int] | None:
    """解析流控窗口更新；格式不匹配时返回 None 交给调用方忽略。"""
    if not data.startswith(WINDOW_UPDATE_PREFIX):
        return None
    payload = data[len(WINDOW_UPDATE_PREFIX):]
    if len(payload) != struct.calcsize('>H I'):
        return None
    return struct.unpack('>H I', payload)


def pack_resize(rows: int, cols: int) -> bytes:
    """打包终端尺寸，供交互式 PTY 调整远端窗口大小。"""
    if not 1 <= rows <= 1000:
        raise ValueError("Terminal rows out of range")
    if not 1 <= cols <= 1000:
        raise ValueError("Terminal columns out of range")
    return RESIZE_PREFIX + struct.pack('>H H', rows, cols)


def unpack_resize(data: bytes) -> tuple[int, int] | None:
    """解析终端尺寸，并过滤明显异常的行列值。"""
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
    """按统一包头格式封装 UDP 消息，并在发送前校验协议边界。"""
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
    """解析 UDP 原始字节；任何包头、类型或长度不合法的包都会被丢弃。"""
    if len(raw_data) < HEADER_SIZE:
        return None
    magic, msg_type, seq, data_len, client_id = struct.unpack(HEADER_FORMAT, raw_data[:HEADER_SIZE])
    if magic != MAGIC or msg_type not in VALID_TYPES:
        return None
    if len(raw_data) != HEADER_SIZE + data_len:
        return None
    data = raw_data[HEADER_SIZE:]
    return (msg_type, seq, client_id, data)
