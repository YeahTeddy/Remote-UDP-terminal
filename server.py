"""UDP 远程终端服务端。

服务端监听 UDP 数据包，维护每个客户端的会话状态，执行普通命令或交互式 PTY
命令，并通过 ACK、滑动窗口、重传和心跳响应在 UDP 上提供可用的远程终端体验。
"""

import codecs
import importlib
import select
import socket
import threading
import time
import subprocess
import os
import signal
import locale
import shlex
import getpass
import platform

# POSIX 使用系统 pty/fcntl/ioctl 管理伪终端；Windows 走 pywinpty 封装。
if os.name != 'nt':
    import fcntl
    import pty
    import struct as winsize_struct
    import termios
else:
    fcntl = None
    pty = None
    winsize_struct = None
    termios = None

from common import *


class WindowsPtySession:
    """封装 pywinpty，提供与 POSIX PTY 相近的 read/write/resize 接口。"""

    def __init__(self, cmd, cwd, rows, cols):
        """启动 Windows PTY 子进程，并兼容不同 pywinpty 版本的参数签名。"""
        try:
            PtyProcess = importlib.import_module('winpty').PtyProcess
        except ImportError as exc:
            raise RuntimeError("Windows PTY requires pywinpty; install it with: pip install pywinpty") from exc

        last_error = None
        # pywinpty 的 spawn 参数在不同版本中不完全一致，因此按能力逐级降级尝试。
        for kwargs in (
            {'cwd': cwd, 'dimensions': (rows, cols)},
            {'cwd': cwd},
            {'dimensions': (rows, cols)},
            {},
        ):
            try:
                self.proc = PtyProcess.spawn(cmd, **kwargs)
                break
            except TypeError as exc:
                last_error = exc
        else:
            raise last_error
        self.stdin_decoder = codecs.getincrementaldecoder('utf-8')(errors='ignore')
        self.resize(rows, cols)

    def read(self, max_bytes=4096, timeout=0.005):
        """读取 PTY 输出；没有数据时返回空字节，便于主循环轮询。"""
        fileobj = getattr(self.proc, 'fileobj', None)
        if fileobj is not None:
            readable, _, _ = select.select([fileobj], [], [], timeout)
            if not readable:
                return b''
        try:
            data = self.proc.read(max_bytes)
        except TypeError:
            data = self.proc.read()
        except Exception:
            if not self.is_alive():
                return b''
            raise
        if data is None:
            return b''
        if isinstance(data, bytes):
            return data
        return data.encode('utf-8', errors='replace')

    def write(self, data):
        """把客户端传来的 UTF-8 字节增量解码后写入 Windows PTY。"""
        text = self.stdin_decoder.decode(data)
        if text:
            self.proc.write(text)

    def resize(self, rows, cols):
        """调用当前 pywinpty 版本支持的尺寸调整方法。"""
        for name in ('setwinsize', 'set_winsize', 'resize'):
            method = getattr(self.proc, name, None)
            if not method:
                continue
            try:
                method(rows, cols)
                return
            except TypeError:
                try:
                    method(cols, rows)
                    return
                except TypeError:
                    continue

    def is_alive(self):
        for name in ('isalive', 'is_alive'):
            method = getattr(self.proc, name, None)
            if method:
                return method()
        return True

    def terminate(self):
        for call in (
            lambda: self.proc.terminate(force=True),
            lambda: self.proc.terminate(),
            lambda: self.proc.kill(),
            lambda: self.proc.close(),
        ):
            try:
                call()
                return
            except (AttributeError, TypeError):
                continue
            except Exception:
                return


class ClientInfo:
    """保存单个客户端的网络、命令执行、流控和 PTY 会话状态。"""

    def __init__(self, addr):
        """为新客户端初始化序列号、工作目录和运行时资源引用。"""
        self.addr = addr
        self.last_heartbeat = time.time()

        # recv_expected_seq 防止重复执行命令；send_seq 负责服务端输出的可靠传输。
        self.recv_expected_seq = 0
        self.send_seq = 0
        self.cwd = os.getcwd()

        # 当前子进程用于 Ctrl+C 中断和客户端清理时终止命令树。
        self.current_process = None
        self.current_command = None
        self.process_lock = threading.Lock()

        # 输出窗口由客户端 ACK 中上报，服务端据此限制未确认的输出包。
        self.send_lock = threading.Lock()
        self.advertised_window_packets = OUTPUT_WINDOW_SIZE
        self.advertised_window_bytes = RECV_BUFFER_LIMIT_BYTES
        self.window_lock = threading.Lock()

        # 交互式命令可能使用 POSIX fd 或 Windows pywinpty session，两者只会存在一个。
        self.pty_fd = None
        self.pty_session = None
        self.pty_lock = threading.Lock()
        self.is_interactive = False
        self.stdin_recv_expected_seq = 0
        self.pending_stdin = []
        self.stdin_event = threading.Event()
        self.term_rows = 24
        self.term_cols = 80


class UDPServer:
    """监听 UDP 请求，执行远端命令，并把输出可靠发送回对应客户端。"""

    def __init__(self, host='0.0.0.0', port=9999):
        """创建 UDP socket，并初始化客户端表、ACK 等待表和服务端提示符信息。"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        # clients 保存所有活动客户端；ack_events 用于输出包等待对应 ACK。
        self.clients = {}
        self.ack_events = {}
        self.ack_lock = threading.Lock()
        self.running = False

        # 普通子进程输出可能不是 UTF-8，必要时按本地编码兜底解码再统一发 UTF-8。
        self.encoding = locale.getpreferredencoding(False) or 'utf-8'
        self.server_user = getpass.getuser()
        self.server_host = platform.node() or socket.gethostname()
        self.server_session_id = os.urandom(8).hex()
        print(f"UDP Server started on {host}:{port}")

    def start(self):
        """启动接收线程和清理线程，并阻塞到用户按 Ctrl+C 停止服务。"""
        self.running = True
        recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        recv_thread.start()
        clean_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        clean_thread.start()
        print("Server running, press Ctrl+C to stop")
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """停止服务端，并释放每个客户端仍在运行的子进程和 PTY 资源。"""
        self.running = False
        for client in list(self.clients.values()):
            self._cleanup_client_runtime(client)
        self.sock.close()
        print("Server stopped")

    def _recv_loop(self):
        """服务端主接收循环，解析 UDP 包并按消息类型分发处理。"""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1500)
                msg = unpack_msg(data)
                if not msg:
                    continue
                msg_type, seq, client_id, payload = msg

                if client_id not in self.clients:
                    # 首次见到 client_id 时创建会话；之后同一 client_id 可更新来源地址以支持端口变化。
                    self.clients[client_id] = ClientInfo(addr)
                    print(f"New client connected: {client_id} from {addr}")

                client = self.clients[client_id]
                client.addr = addr
                client.last_heartbeat = time.time()

                if msg_type == TYPE_HEARTBEAT:
                    self._handle_heartbeat(client_id, seq, addr)
                elif msg_type == TYPE_ACK:
                    self._handle_ack(client_id, seq, payload)
                elif msg_type == TYPE_COMMAND:
                    self._handle_command(client_id, seq, payload, addr)
                elif msg_type == TYPE_INTERRUPT:
                    self._handle_interrupt(client_id)
                elif msg_type == TYPE_WINDOW_UPDATE:
                    self._handle_window_update(client_id, payload)
                elif msg_type == TYPE_RESIZE:
                    self._handle_resize(client_id, seq, payload, addr)
                elif msg_type == TYPE_STDIN:
                    self._handle_stdin(client_id, seq, payload, addr)
            except Exception as e:
                if self.running:
                    print(f"Recv error: {e}")

    def _handle_heartbeat(self, client_id, seq, addr):
        """回复心跳 ACK，并把远端提示符信息同步给客户端。"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] Heartbeat received from client {client_id} at {addr}")
        client = self.clients[client_id]
        payload = pack_prompt_info(self.server_user, self.server_host, client.cwd, self.server_session_id)
        ack_msg = pack_msg(TYPE_ACK, seq, client_id, payload)
        self.sock.sendto(ack_msg, addr)

    def _handle_ack(self, client_id, seq, payload=b''):
        """处理客户端 ACK：先更新接收窗口，再唤醒等待该序列号的发送线程。"""
        self._apply_window_update(client_id, payload)
        with self.ack_lock:
            key = (client_id, seq)
            if key in self.ack_events:
                self.ack_events[key].set()

    def _apply_window_update(self, client_id, payload_or_update):
        """应用客户端声明的可接收窗口，控制后续输出发送节奏。"""
        if isinstance(payload_or_update, tuple):
            update = payload_or_update
        else:
            update = unpack_window_update(payload_or_update)
        if not update:
            return
        packets, bytes_available = update
        client = self.clients.get(client_id)
        if not client:
            return
        with client.window_lock:
            client.advertised_window_packets = max(0, min(OUTPUT_WINDOW_SIZE, packets))
            client.advertised_window_bytes = max(0, bytes_available)

    def _handle_window_update(self, client_id, payload):
        self._apply_window_update(client_id, payload)

    def _get_effective_send_window(self, client):
        with client.window_lock:
            return max(0, min(OUTPUT_WINDOW_SIZE, client.advertised_window_packets))

    def _kill_process_tree(self, proc):
        """强制结束子进程及其子进程，避免远端命令在客户端断开后残留。"""
        try:
            if os.name == 'nt':
                subprocess.run(
                    ['taskkill', '/F', '/T', '/PID', str(proc.pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False
                )
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    def _interrupt_process(self, proc, cmd=None):
        """把客户端 Ctrl+C 转换为当前平台的子进程中断动作。"""
        try:
            if os.name == 'nt':
                proc.send_signal(signal.CTRL_BREAK_EVENT)
                time.sleep(0.05 if self._is_ping_command(cmd) else 0.2)
                if proc.poll() is None:
                    self._kill_process_tree(proc)
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        except Exception:
            self._kill_process_tree(proc)

    def _write_pty(self, client, payload):
        """把客户端 stdin 写入当前交互式 PTY，自动区分 Windows session 和 POSIX fd。"""
        try:
            with client.pty_lock:
                if client.pty_session is not None:
                    client.pty_session.write(payload)
                    return True
                if client.pty_fd is not None:
                    os.write(client.pty_fd, payload)
                    return True
        except Exception:
            return False
        return False

    def _handle_interrupt(self, client_id):
        """处理中断消息：交互式命令写入 Ctrl+C，普通命令中断进程组。"""
        client = self.clients.get(client_id)
        if not client:
            return

        if client.is_interactive and self._write_pty(client, b'\x03'):
            return

        with client.process_lock:
            proc = client.current_process
            cmd = client.current_command

        if proc and proc.poll() is None:
            self._interrupt_process(proc, cmd)

    def _handle_stdin(self, client_id, seq, payload, addr):
        """接收交互式 stdin，ACK 后按序放入 PTY 待写队列。"""
        client = self.clients.get(client_id)
        if not client:
            return

        ack_msg = pack_msg(TYPE_ACK, seq, client_id, b'')
        self.sock.sendto(ack_msg, addr)

        expected = client.stdin_recv_expected_seq
        # stdin 允许跳到更新的序列号，避免交互式输入因丢包长期卡住。
        if seq == expected:
            client.stdin_recv_expected_seq = next_data_seq(expected)
        elif is_sequence_ahead(seq, expected):
            client.stdin_recv_expected_seq = next_data_seq(seq)
        else:
            return

        with client.pty_lock:
            if client.is_interactive and len(client.pending_stdin) < 128:
                client.pending_stdin.append(payload)
                client.stdin_event.set()

    def _flush_pending_stdin(self, client):
        """取出累计的 stdin 片段并一次性写入 PTY，减少锁内 I/O 时间。"""
        with client.pty_lock:
            pending = client.pending_stdin
            client.pending_stdin = []
            client.stdin_event.clear()
        if pending:
            self._write_pty(client, b''.join(pending))

    def _set_pty_size_fd(self, fd, rows, cols):
        """在 POSIX 上通过 TIOCSWINSZ 更新 PTY 的行列数。"""
        if os.name == 'nt' or fd is None:
            return
        try:
            fcntl.ioctl(fd, termios.TIOCSWINSZ, winsize_struct.pack('HHHH', rows, cols, 0, 0))
        except Exception:
            pass

    def _resize_client_pty(self, client, rows, cols):
        with client.pty_lock:
            session = client.pty_session
            fd = client.pty_fd
        if session is not None:
            try:
                session.resize(rows, cols)
            except Exception:
                pass
        elif fd is not None:
            self._set_pty_size_fd(fd, rows, cols)

    def _handle_resize(self, client_id, seq, payload, addr):
        """保存客户端终端尺寸，并同步调整当前正在运行的 PTY。"""
        client = self.clients.get(client_id)
        if not client:
            return
        size = unpack_resize(payload)
        if not size:
            return
        rows, cols = size
        client.term_rows = rows
        client.term_cols = cols
        self._resize_client_pty(client, rows, cols)
        ack_msg = pack_msg(TYPE_ACK, seq, client_id, b'')
        self.sock.sendto(ack_msg, addr)

    def _send_packet(self, client_id, addr, seq, chunk):
        msg = pack_msg(TYPE_OUTPUT, seq, client_id, chunk)
        self.sock.sendto(msg, addr)

    def _send_output_chunks_reliable(self, client_id, chunks):
        """使用滑动窗口可靠发送输出分片，直到全部 ACK 或客户端不可用。"""
        if not chunks:
            return True

        client = self.clients.get(client_id)
        if not client:
            return False

        with client.send_lock:
            packets = []
            # 为每个输出分片预分配序列号和等待事件，后续重传始终复用同一序列号。
            for chunk in chunks:
                seq = client.send_seq
                ack_event = threading.Event()
                with self.ack_lock:
                    self.ack_events[(client_id, seq)] = ack_event
                packets.append({
                    'seq': seq,
                    'chunk': chunk,
                    'event': ack_event,
                    'sent': False,
                    'acked': False,
                    'retries': 0,
                    'last_sent': 0.0,
                })
                client.send_seq = next_data_seq(client.send_seq)

            # base 是窗口内最早未确认包，next_to_send 是下一个尚未首次发送的包。
            base = 0
            next_to_send = 0
            last_progress = time.monotonic()
            try:
                while base < len(packets) and self.running:
                    if client_id not in self.clients:
                        return False
                    ack_to = None
                    for index in range(base, next_to_send):
                        if packets[index]['event'].is_set():
                            ack_to = index
                    # 客户端按 Go-Back-N 回累计 ACK，因此确认 ack_to 之前的所有包。
                    if ack_to is not None:
                        for packet in packets[base:ack_to + 1]:
                            packet['acked'] = True
                        last_progress = time.monotonic()

                    while base < len(packets) and packets[base]['acked']:
                        with self.ack_lock:
                            self.ack_events.pop((client_id, packets[base]['seq']), None)
                        base += 1
                        last_progress = time.monotonic()

                    if base >= len(packets):
                        return True

                    effective_window = self._get_effective_send_window(client)
                    # 只在客户端声明的窗口内发送新包，避免接收端缓冲区被输出洪峰压满。
                    while next_to_send < len(packets) and next_to_send - base < effective_window:
                        packet = packets[next_to_send]
                        self._send_packet(client_id, client.addr, packet['seq'], packet['chunk'])
                        packet['sent'] = True
                        packet['last_sent'] = time.monotonic()
                        next_to_send += 1
                        last_progress = time.monotonic()

                    now = time.monotonic()
                    timed_out = (
                        next_to_send > base
                        and packets[base]['sent']
                        and now - packets[base]['last_sent'] >= ACK_TIMEOUT
                    )
                    if timed_out:
                        # 最早未确认包超时后，重传当前窗口内所有已发送但未累计确认的包。
                        for packet in packets[base:next_to_send]:
                            packet['retries'] += 1
                            self._send_packet(client_id, client.addr, packet['seq'], packet['chunk'])
                            packet['last_sent'] = time.monotonic()
                            last_progress = time.monotonic()
                            if packet['retries'] % MAX_RETRIES == 0:
                                print(f"Retry {packet['retries']} for client {client_id} seq {packet['seq']}")
                        continue

                    if next_to_send == base and effective_window <= 0:
                        # 窗口长期为 0 说明客户端无法继续接收，避免发送线程永久占用。
                        if now - last_progress >= FLOW_CONTROL_IDLE_TIMEOUT:
                            print(f"Flow control timeout for client {client_id}")
                            return False
                    time.sleep(0.001)
            except Exception as e:
                print(f"Send error: {e}")
                return False
            finally:
                with self.ack_lock:
                    for packet in packets:
                        self.ack_events.pop((client_id, packet['seq']), None)

        return False

    def _send_output_reliable(self, client_id, data):
        """把任意长度输出拆成协议分片后可靠发送。"""
        chunks = [data[i:i + MAX_DATA_SIZE] for i in range(0, len(data), MAX_DATA_SIZE)]
        return self._send_output_chunks_reliable(client_id, chunks)

    def _send_output_done(self, client_id):
        """发送命令结束标记，顺带同步客户端提示符中的当前目录。"""
        client = self.clients.get(client_id)
        if not client:
            return False
        return self._send_output_chunks_reliable(client_id, [pack_output_done(client.cwd)])

    def _send_reliable(self, client_id, data):
        """发送一次完整响应：输出分片之后追加结束标记。"""
        chunks = [data[i:i + MAX_DATA_SIZE] for i in range(0, len(data), MAX_DATA_SIZE)]
        chunks.append(pack_output_done(self.clients[client_id].cwd))
        return self._send_output_chunks_reliable(client_id, chunks)

    def _handle_command(self, client_id, seq, payload, addr):
        """处理命令消息，先 ACK 和去重，再按 cd/普通命令/PTY 命令分流执行。"""
        client = self.clients[client_id]

        if seq != client.recv_expected_seq:
            # 重复或乱序命令只回 ACK，不再次执行，避免 UDP 重传造成副作用。
            ack_msg = pack_msg(TYPE_ACK, seq, client_id, b'')
            self.sock.sendto(ack_msg, addr)
            return

        client.recv_expected_seq = next_data_seq(client.recv_expected_seq)
        ack_msg = pack_msg(TYPE_ACK, seq, client_id, b'')
        self.sock.sendto(ack_msg, addr)

        text = payload.decode('utf-8', errors='replace')
        cmd = normalize_command_input(text).rstrip('\r\n')
        if not cmd:
            return

        if self._is_cd_command(cmd):
            # cd 必须改变服务端保存的会话目录，不能只在一次 shell 子进程里执行。
            threading.Thread(
                target=self._change_directory_and_respond,
                args=(client_id, cmd),
                daemon=True
            ).start()
            return

        if should_use_pty_command(cmd):
            with client.pty_lock:
                client.is_interactive = True
                client.pending_stdin = []
                client.stdin_event.clear()
            target = self._execute_pty_and_respond
        else:
            target = self._execute_and_respond
        threading.Thread(target=target, args=(client_id, cmd), daemon=True).start()

    def _split_command(self, cmd):
        """按平台 shell 规则拆分命令，用于识别内置命令和特殊程序。"""
        try:
            return shlex.split(cmd, posix=os.name != 'nt')
        except ValueError:
            return cmd.split()

    def _is_cd_command(self, cmd):
        """识别单独的 cd 命令，排除管道、串联等需要交给 shell 的复合命令。"""
        tokens = self._split_command(cmd)
        return bool(tokens) and tokens[0].lower() == 'cd' and not any(token in {'&', '&&', '|', '||', ';'} for token in tokens)

    def _is_ping_command(self, cmd):
        """识别 ping，用于 Windows 上处理 Ctrl+Break 后额外输出的特殊情况。"""
        tokens = self._split_command(cmd or '')
        if not tokens:
            return False
        exe = os.path.basename(tokens[0]).lower()
        if exe.endswith('.exe'):
            exe = exe[:-4]
        return exe == 'ping'

    def _change_directory_and_respond(self, client_id, cmd):
        """在客户端会话中持久化工作目录，并向客户端发送空输出或错误信息。"""
        client = self.clients.get(client_id)
        if not client:
            return

        tokens = self._split_command(cmd)
        args = tokens[1:]
        if os.name == 'nt' and args and args[0].lower() == '/d':
            args = args[1:]

        if not args:
            target = os.path.expanduser('~')
        else:
            # 相对路径基于该客户端保存的 cwd 解析，而不是服务端进程自己的 cwd。
            target = ' '.join(args).strip('"\'')
            target = os.path.expandvars(os.path.expanduser(target))
            if not os.path.isabs(target):
                target = os.path.join(client.cwd, target)

        try:
            new_cwd = os.path.abspath(target)
            if not os.path.isdir(new_cwd):
                output = f"cd: no such file or directory: {target}\n"
            else:
                client.cwd = new_cwd
                output = ''
        except Exception as e:
            output = f"cd: {e}\n"

        if client_id in self.clients:
            self._send_reliable(client_id, output.encode('utf-8'))

    def _normalize_interrupt_text(self, text, stop_after_marker=False):
        """统一 Windows 中断提示文本，并可截断 ping 在中断后继续输出的尾部。"""
        text = text.replace('Control-Break', 'Control-C')
        if not stop_after_marker:
            return text, False
        marker_index = text.find('Control-C')
        if marker_index < 0:
            return text, False
        end = marker_index + len('Control-C')
        while end < len(text) and text[end] in '\r\n':
            end += 1
        return text[:end], True

    def _stream_pipe(self, client_id, pipe, cmd=None, output_failed_event=None):
        """持续读取子进程 stdout/stderr，边产生边可靠转发给客户端。"""
        try:
            # 先按 UTF-8 尝试流式解码；遇到非法字节后切换到系统本地编码。
            utf8_decoder = codecs.getincrementaldecoder('utf-8')(errors='strict')
            local_decoder = codecs.getincrementaldecoder(self.encoding)(errors='replace')
            use_utf8 = True
            suppress_after_interrupt = os.name == 'nt' and self._is_ping_command(cmd)
            read_chunk = pipe.read1 if hasattr(pipe, 'read1') else pipe.read
            while self.running:
                chunk = read_chunk(4096)
                if not chunk:
                    break
                if use_utf8:
                    pending = utf8_decoder.getstate()[0]
                    try:
                        text = utf8_decoder.decode(chunk)
                    except UnicodeDecodeError:
                        use_utf8 = False
                        text = local_decoder.decode(pending + chunk)
                else:
                    text = local_decoder.decode(chunk)
                if os.name == 'nt':
                    # Windows ping 在 Ctrl+Break 后可能继续打印 reply，客户端测试要求截断这些尾部。
                    text, interrupted = self._normalize_interrupt_text(text, suppress_after_interrupt)
                    if interrupted:
                        if text and not self._send_output_reliable(client_id, text.encode('utf-8')):
                            if output_failed_event is not None:
                                output_failed_event.set()
                        break
                if text and not self._send_output_reliable(client_id, text.encode('utf-8')):
                    if output_failed_event is not None:
                        output_failed_event.set()
                    break
            if use_utf8:
                pending = utf8_decoder.getstate()[0]
                try:
                    tail = utf8_decoder.decode(b'', final=True)
                except UnicodeDecodeError:
                    tail = local_decoder.decode(pending, final=True)
            else:
                tail = local_decoder.decode(b'', final=True)
            if os.name == 'nt':
                tail, _ = self._normalize_interrupt_text(tail, suppress_after_interrupt)
            if tail and not self._send_output_reliable(client_id, tail.encode('utf-8')):
                if output_failed_event is not None:
                    output_failed_event.set()
        except Exception as e:
            if self.running:
                print(f"Stream error: {e}")
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    def _execute_and_respond(self, client_id, cmd):
        """执行普通非交互命令，实时转发输出，并在结束时发送完成标记。"""
        client = self.clients.get(client_id)
        if not client:
            return

        proc = None
        reader_threads = []
        output_failed_event = threading.Event()
        try:
            if os.name == 'nt':
                # Windows 通过 CREATE_NEW_PROCESS_GROUP 支持后续发送 CTRL_BREAK_EVENT。
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=client.cwd,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                    shell=True
                )
            else:
                # POSIX 创建新进程组，Ctrl+C 时可以中断整棵命令进程组。
                proc = subprocess.Popen(
                    ['bash', '-c', cmd],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=client.cwd,
                    preexec_fn=os.setsid
                )

            with client.process_lock:
                client.current_process = proc
                client.current_command = cmd

            for pipe in (proc.stdout, proc.stderr):
                # stdout/stderr 分别开线程读取，避免任一管道缓冲区满导致子进程阻塞。
                if pipe is None:
                    continue
                thread = threading.Thread(target=self._stream_pipe, args=(client_id, pipe, cmd, output_failed_event), daemon=True)
                thread.start()
                reader_threads.append(thread)

            while proc.poll() is None:
                if output_failed_event.is_set():
                    self._kill_process_tree(proc)
                    break
                time.sleep(0.1)

            for thread in reader_threads:
                thread.join()
        except Exception as e:
            self._send_output_reliable(client_id, f"Error: {str(e)}\n".encode('utf-8'))
        finally:
            if proc is not None:
                with client.process_lock:
                    if client.current_process == proc:
                        client.current_process = None
                        client.current_command = None
            if client_id in self.clients:
                self._send_output_done(client_id)

    def _execute_pty_and_respond(self, client_id, cmd):
        """进入交互式命令路径，并按平台选择 POSIX PTY 或 Windows pywinpty。"""
        cmd = strip_pty_prefix(cmd)
        if not cmd:
            self._send_reliable(client_id, b"Error: empty PTY command\n")
            return
        if os.name == 'nt':
            self._execute_windows_pty_and_respond(client_id, cmd)
        else:
            self._execute_posix_pty_and_respond(client_id, cmd)

    def _execute_posix_pty_and_respond(self, client_id, cmd):
        """在 POSIX 上创建伪终端运行交互式命令，并双向转发 PTY 数据。"""
        client = self.clients.get(client_id)
        if not client:
            return

        master_fd = None
        slave_fd = None
        proc = None
        try:
            master_fd, slave_fd = pty.openpty()
            self._set_pty_size_fd(slave_fd, client.term_rows, client.term_cols)
            # 子进程的 stdin/stdout/stderr 都连接到 PTY slave，服务端从 master 端读写。
            proc = subprocess.Popen(
                ['bash', '-lc', cmd],
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                cwd=client.cwd,
                preexec_fn=os.setsid,
                close_fds=True
            )
            os.close(slave_fd)
            slave_fd = None

            with client.process_lock:
                client.current_process = proc
            # 记录 PTY 句柄后，后续 stdin/resize/interrupt 消息才能定位到当前交互会话。
            with client.pty_lock:
                client.pty_fd = master_fd
                client.pty_session = None
                client.is_interactive = True
                client.stdin_recv_expected_seq = 0
            self._flush_pending_stdin(client)

            while self.running and proc.poll() is None:
                # 每轮先写入客户端累计 stdin，再读取 PTY 输出并可靠发送给客户端。
                self._flush_pending_stdin(client)
                readable, _, _ = select.select([master_fd], [], [], 0.1)
                if not readable:
                    continue
                try:
                    chunk = os.read(master_fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                if not self._send_output_reliable(client_id, chunk):
                    break

            try:
                # 子进程退出后再非阻塞排空一次 PTY，避免最后一段输出丢失。
                while True:
                    readable, _, _ = select.select([master_fd], [], [], 0)
                    if not readable:
                        break
                    chunk = os.read(master_fd, 4096)
                    if not chunk:
                        break
                    if not self._send_output_reliable(client_id, chunk):
                        break
            except OSError:
                pass

            try:
                proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self._kill_process_tree(proc)
        except Exception as e:
            self._send_output_reliable(client_id, f"Error: {str(e)}\n".encode('utf-8'))
        finally:
            with client.pty_lock:
                if client.pty_fd == master_fd:
                    client.pty_fd = None
                client.pty_session = None
                client.is_interactive = False
                client.pending_stdin = []
                client.stdin_event.clear()
            with client.process_lock:
                if proc is not None and client.current_process == proc:
                    client.current_process = None
            for fd in (master_fd, slave_fd):
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
            if client_id in self.clients:
                self._send_output_done(client_id)

    def _read_available_windows_pty_output(self, session, first_chunk):
        """合并 Windows PTY 当前已就绪的输出，尽量填满一个 UDP 分片。"""
        chunks = [first_chunk]
        total = len(first_chunk)
        while total < MAX_DATA_SIZE:
            chunk = session.read(MAX_DATA_SIZE - total, timeout=0)
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        return b''.join(chunks)

    def _execute_windows_pty_and_respond(self, client_id, cmd):
        """在 Windows 上通过 pywinpty 运行交互式命令并转发输入输出。"""
        client = self.clients.get(client_id)
        if not client:
            return

        session = None
        try:
            session = WindowsPtySession(cmd, client.cwd, client.term_rows, client.term_cols)
            # Windows PTY 没有 POSIX fd，因此保存 session 对象供 stdin/resize/interrupt 使用。
            with client.pty_lock:
                client.pty_session = session
                client.pty_fd = None
                client.is_interactive = True
                client.stdin_recv_expected_seq = 0
            self._flush_pending_stdin(client)

            while self.running and session.is_alive():
                # pywinpty 输出采用短超时轮询，同时把客户端积累的 stdin 写回远端程序。
                self._flush_pending_stdin(client)
                chunk = session.read(4096)
                if chunk:
                    chunk = self._read_available_windows_pty_output(session, chunk)
                    if not self._send_output_reliable(client_id, chunk):
                        break
                elif not session.is_alive():
                    break

            # 会话结束后短暂 drain，收集退出前最后刷出的提示符或输出。
            drain_until = time.monotonic() + 1
            while self.running and time.monotonic() < drain_until:
                chunk = session.read(4096, timeout=0.1)
                if not chunk:
                    continue
                chunk = self._read_available_windows_pty_output(session, chunk)
                drain_until = time.monotonic() + 0.2
                if not self._send_output_reliable(client_id, chunk):
                    break
        except Exception as e:
            self._send_output_reliable(client_id, f"Error: {str(e)}\n".encode('utf-8'))
        finally:
            with client.pty_lock:
                if client.pty_session == session:
                    client.pty_session = None
                client.is_interactive = False
                client.pending_stdin = []
                client.stdin_event.clear()
            if session is not None:
                try:
                    session.terminate()
                except Exception:
                    pass
            if client_id in self.clients:
                self._send_output_done(client_id)

    def _cleanup_client_runtime(self, client):
        """清理客户端运行时资源，包括 PTY、待输入和仍在运行的命令进程。"""
        with client.pty_lock:
            session = client.pty_session
            fd = client.pty_fd
            client.pty_session = None
            client.pty_fd = None
            client.is_interactive = False
            client.pending_stdin = []
            client.stdin_event.clear()
        if session is not None:
            try:
                session.terminate()
            except Exception:
                pass
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        with client.process_lock:
            proc = client.current_process
            client.current_process = None
            client.current_command = None
        if proc and proc.poll() is None:
            self._kill_process_tree(proc)

    def _cleanup_loop(self):
        """定期移除长时间没有心跳的客户端，并释放其远端资源。"""
        while self.running:
            now = time.time()
            to_remove = []
            for cid, client in list(self.clients.items()):
                if now - client.last_heartbeat > 30:
                    self._cleanup_client_runtime(client)
                    to_remove.append(cid)
            for cid in to_remove:
                print(f"Client {cid} timed out, removed")
                del self.clients[cid]

            time.sleep(5)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='UDP Remote Terminal Server')
    parser.add_argument('port', nargs='?', type=int, default=9999, help='Listen port')
    parser.add_argument('--host', default='0.0.0.0', help='Listen host')
    args = parser.parse_args()
    server = UDPServer(args.host, args.port)
    server.start()
