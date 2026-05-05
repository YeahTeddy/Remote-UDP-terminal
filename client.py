import socket
import threading
import time
import random
import signal
import sys
import os
import shutil
import _thread

if os.name == 'nt':
    import ctypes
    import msvcrt
    from ctypes import wintypes

    STD_INPUT_HANDLE = -10
    KEY_EVENT = 0x0001
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

    class _WindowsCharUnion(ctypes.Union):
        _fields_ = [
            ('UnicodeChar', wintypes.WCHAR),
            ('AsciiChar', ctypes.c_char),
        ]

    class _WindowsKeyEventRecord(ctypes.Structure):
        _fields_ = [
            ('bKeyDown', wintypes.BOOL),
            ('wRepeatCount', wintypes.WORD),
            ('wVirtualKeyCode', wintypes.WORD),
            ('wVirtualScanCode', wintypes.WORD),
            ('uChar', _WindowsCharUnion),
            ('dwControlKeyState', wintypes.DWORD),
        ]

    class _WindowsInputEvent(ctypes.Union):
        _fields_ = [
            ('KeyEvent', _WindowsKeyEventRecord),
            ('RawEvent', ctypes.c_byte * 20),
        ]

    class _WindowsInputRecord(ctypes.Structure):
        _fields_ = [
            ('EventType', wintypes.WORD),
            ('Event', _WindowsInputEvent),
        ]

    _kernel32 = ctypes.windll.kernel32
    _kernel32.GetStdHandle.argtypes = [wintypes.DWORD]
    _kernel32.GetStdHandle.restype = wintypes.HANDLE
    _kernel32.GetNumberOfConsoleInputEvents.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    _kernel32.GetNumberOfConsoleInputEvents.restype = wintypes.BOOL
    _kernel32.ReadConsoleInputW.argtypes = [wintypes.HANDLE, ctypes.POINTER(_WindowsInputRecord), wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    _kernel32.ReadConsoleInputW.restype = wintypes.BOOL
else:
    import select
    import termios
    import tty

from common import *


class UDPClient:
    def __init__(self, server_host='127.0.0.1', server_port=9999):
        self.server_addr = (server_host, server_port)
        self.client_id = random.randint(1, 2**32 - 1)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.5)
        self.running = False
        self.send_seq = 0
        self.recv_expected_seq = 0
        self.recv_buffer = {}
        self.recv_buffer_bytes = 0
        self.recv_buffer_limit_packets = RECV_BUFFER_LIMIT_PACKETS
        self.recv_buffer_limit_bytes = RECV_BUFFER_LIMIT_BYTES
        self.ack_event = threading.Event()
        self.output_done_event = threading.Event()
        self.heartbeat_ack_event = threading.Event()
        self.last_heartbeat_ack = 0
        self.waiting_seq = -1
        self.stdin_seq = DATA_SEQUENCE_MOD // 3
        self.window_update_seq = DATA_SEQUENCE_MOD // 2
        self.resize_seq = DATA_SEQUENCE_MOD * 2 // 3
        self.print_lock = threading.Lock()
        self.prompt_ready_event = threading.Event()
        self.prompt_user = 'user'
        self.prompt_host = server_host
        self.prompt_dir = '~'
        self.prompt_interrupt_seen = False
        self.suppress_interrupt_until = 0
        self.saved_sigint_handler = None
        self.saved_sigwinch_handler = None
        self.resume_sigint_at = 0
        self.connection_error_reported = False
        self.connected = False
        self.interactive_mode = False
        self.command_waiting = False
        self.interrupt_after_reconnect = False
        self.reconnect_interrupt_reported = False
        self.reconnect_interrupt_started_at = 0
        self.last_reconnect_interrupt_sent = 0
        self.raw_terminal_attrs = None
        self.pending_windows_high_surrogate = None
        self.windows_stdin_handle = self._get_windows_stdin_handle()
        self.last_rows = 24
        self.last_cols = 80
        self.stop_event = threading.Event()
        self.recv_thread = None
        self.heartbeat_thread = None
        print(f"Client ID: {self.client_id}, connecting to {server_host}:{server_port}")

    def _format_prompt_dir(self, path):
        normalized = path.replace('\\', '/').rstrip('/')
        if not normalized:
            return '/'
        return normalized.rsplit('/', 1)[-1]

    def _prompt(self):
        suffix = '#' if self.prompt_user in ('root', 'Administrator') else '$'
        return f"[{self.prompt_user}@{self.prompt_host} {self.prompt_dir}]{suffix} "

    def _safe_print(self, text=''):
        try:
            print(text)
        except KeyboardInterrupt:
            pass

    def _print_interrupt_marker(self):
        if sys.stdin.isatty() and sys.platform != 'win32':
            self._safe_print()
        else:
            self._safe_print("^C")

    def _suppress_sigint_briefly(self):
        try:
            if self.saved_sigint_handler is None:
                self.saved_sigint_handler = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            self.resume_sigint_at = time.monotonic() + 0.05
        except (ValueError, AttributeError):
            pass

    def _resume_sigint(self):
        if self.saved_sigint_handler is None:
            return
        delay = self.resume_sigint_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        try:
            signal.signal(signal.SIGINT, self.saved_sigint_handler)
        except (ValueError, AttributeError):
            pass
        self.saved_sigint_handler = None

    def _note_prompt_interrupt(self):
        self.prompt_interrupt_seen = True
        self._print_interrupt_marker()

    def _handle_prompt_interrupt(self):
        self._suppress_sigint_briefly()
        self._note_prompt_interrupt()

    def _begin_command_interrupt_window(self):
        if self.prompt_interrupt_seen:
            self.suppress_interrupt_until = time.monotonic() + 1
            self.prompt_interrupt_seen = False
        else:
            self.suppress_interrupt_until = 0

    def _is_suppressed_interrupt(self):
        return time.monotonic() < self.suppress_interrupt_until

    def _send_command_reliable(self, data):
        while self.running:
            try:
                return self._send_reliable(data)
            except KeyboardInterrupt:
                if not self.running:
                    return False
                if self._is_suppressed_interrupt():
                    continue
                self._print_interrupt_marker()
                self._send_interrupt()
                return True
        return False

    def start(self):
        self.running = True
        self.stop_event.clear()
        self.recv_thread = threading.Thread(target=self._recv_loop)
        self.recv_thread.start()
        try:
            connected = self._connect_to_server()
        except KeyboardInterrupt:
            self._shutdown_runtime()
            return
        if not connected:
            self._safe_print(f"Connection failed: server {self.server_addr[0]}:{self.server_addr[1]} did not respond")
            self._shutdown_runtime()
            return

        self.connected = True
        self._install_resize_handler()
        self._send_resize()
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
        self.heartbeat_thread.start()
        print("Connected to server, enter commands to execute, 'exit' to quit")

        try:
            while self.running:
                try:
                    self._resume_sigint()
                    cmd = input(self._prompt())
                except KeyboardInterrupt:
                    if not self.running:
                        break
                    self._handle_prompt_interrupt()
                    continue
                except EOFError:
                    if sys.stdin.isatty():
                        self._handle_prompt_interrupt()
                        continue
                    self.stop()
                    break

                if cmd.strip() == 'exit':
                    self.stop()
                    break
                if not cmd.strip():
                    continue

                is_pty = should_use_pty_command(cmd)
                self.output_done_event.clear()
                self._begin_command_interrupt_window()
                if is_pty:
                    self._send_resize()
                    self.interactive_mode = True
                sent = self._send_command_reliable((cmd + '\n').encode('utf-8'))
                if not sent:
                    self.interactive_mode = False
                    self._restore_terminal_mode()
                    continue
                if is_pty:
                    self._run_interactive_until_done()
                else:
                    self._wait_for_command_output()
        except KeyboardInterrupt:
            if self.running:
                self._handle_prompt_interrupt()
        finally:
            self._restore_terminal_mode()
            self._restore_resize_handler()
            self._shutdown_runtime()

    def _mark_command_connection_interrupted(self):
        if self.command_waiting and not self.interactive_mode:
            self.interrupt_after_reconnect = True
            self.reconnect_interrupt_reported = False
            self.reconnect_interrupt_started_at = 0
            self.last_reconnect_interrupt_sent = 0

    def _interrupt_command_after_reconnect_if_needed(self):
        if not self.interrupt_after_reconnect or self.interactive_mode:
            return
        now = time.monotonic()
        if self.reconnect_interrupt_started_at and now - self.reconnect_interrupt_started_at >= 3:
            with self.print_lock:
                print("\nNo command completion received after reconnect interrupt, returning to prompt")
            self.output_done_event.set()
            return
        if now - self.last_heartbeat_ack > 3:
            return
        if now - self.last_reconnect_interrupt_sent < 1:
            return
        if not self.reconnect_interrupt_reported:
            with self.print_lock:
                print("\nConnection restored, interrupting current command")
            self.reconnect_interrupt_reported = True
            self.reconnect_interrupt_started_at = now
        self._send_interrupt()
        self.last_reconnect_interrupt_sent = now

    def _wait_for_command_output(self):
        self.command_waiting = True
        try:
            while self.running and not self.output_done_event.is_set():
                self._interrupt_command_after_reconnect_if_needed()
                try:
                    self.output_done_event.wait(0.1)
                except KeyboardInterrupt:
                    if not self.running:
                        break
                    if self._is_suppressed_interrupt():
                        continue
                    if os.name != 'nt':
                        self._print_interrupt_marker()
                    self._send_interrupt()
        finally:
            self.command_waiting = False
            self.interrupt_after_reconnect = False
            self.reconnect_interrupt_reported = False
            self.reconnect_interrupt_started_at = 0
            self.last_reconnect_interrupt_sent = 0

    def _close_socket(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def _wake_waiters(self):
        self.ack_event.set()
        self.output_done_event.set()
        self.heartbeat_ack_event.set()

    def _join_background_threads(self):
        current = threading.current_thread()
        for thread in (self.recv_thread, self.heartbeat_thread):
            if thread is not None and thread is not current and thread.is_alive():
                try:
                    thread.join(timeout=2)
                except KeyboardInterrupt:
                    pass

    def _shutdown_runtime(self):
        self.running = False
        self.connected = False
        self.stop_event.set()
        self._wake_waiters()
        self._close_socket()
        self._join_background_threads()

    def stop(self):
        if not self.running:
            return
        self._resume_sigint()
        self._restore_terminal_mode()
        self._restore_resize_handler()
        self._shutdown_runtime()
        self._safe_print("\nClient stopped")

    def _send_interrupt(self):
        try:
            msg = pack_msg(TYPE_INTERRUPT, self.send_seq, self.client_id, b'')
            self.sock.sendto(msg, self.server_addr)
        except Exception:
            pass

    def _send_heartbeat(self):
        try:
            msg = pack_msg(TYPE_HEARTBEAT, HEARTBEAT_SEQ, self.client_id, b'')
            self.sock.sendto(msg, self.server_addr)
            return True
        except Exception:
            return False

    def _wait_for_heartbeat_ack(self, timeout=2):
        self.heartbeat_ack_event.clear()
        sent_at = time.monotonic()
        if not self._send_heartbeat():
            return False

        deadline = sent_at + timeout
        while self.running:
            if self.last_heartbeat_ack >= sent_at:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self.heartbeat_ack_event.wait(remaining)
        return False

    def _connect_to_server(self, timeout=2):
        return self._wait_for_heartbeat_ack(timeout=timeout)

    def _handle_connection_error(self):
        if self.connection_error_reported:
            return
        self.connection_error_reported = True
        self._restore_terminal_mode()
        self._safe_print("\nConnection error: server did not respond to heartbeat, exiting")
        self._shutdown_runtime()
        _thread.interrupt_main()

    def _heartbeat_loop(self):
        missed_heartbeats = 0
        max_missed_heartbeats = 3
        while self.running:
            retry_delay = 2 if missed_heartbeats else 5
            if self.stop_event.wait(retry_delay):
                break
            if not self.running:
                break
            if self._wait_for_heartbeat_ack(timeout=2):
                missed_heartbeats = 0
                continue
            missed_heartbeats += 1
            self._mark_command_connection_interrupted()
            if missed_heartbeats >= max_missed_heartbeats:
                self._handle_connection_error()
                break

    def _apply_prompt_info(self, user, host, cwd):
        if user:
            self.prompt_user = user
        if host:
            self.prompt_host = host
        if cwd:
            self.prompt_dir = self._format_prompt_dir(cwd)
        self.prompt_ready_event.set()

    def _available_recv_window_packets(self):
        return max(0, self.recv_buffer_limit_packets - len(self.recv_buffer))

    def _available_recv_window_bytes(self):
        return max(0, self.recv_buffer_limit_bytes - self.recv_buffer_bytes)

    def _make_window_update_payload(self):
        return pack_window_update(self._available_recv_window_packets(), self._available_recv_window_bytes())

    def _send_window_update(self):
        try:
            msg = pack_msg(TYPE_WINDOW_UPDATE, self.window_update_seq, self.client_id, self._make_window_update_payload())
            self.sock.sendto(msg, self.server_addr)
            self.window_update_seq = next_data_seq(self.window_update_seq)
        except Exception:
            pass

    def _process_output_payload(self, payload):
        done_cwd = unpack_output_done(payload)
        if done_cwd is not None:
            if done_cwd:
                self.prompt_dir = self._format_prompt_dir(done_cwd)
            self.output_done_event.set()
            return

        with self.print_lock:
            if self.interactive_mode:
                out = getattr(sys.stdout, 'buffer', None)
                if out is not None:
                    out.write(payload)
                    out.flush()
                else:
                    sys.stdout.write(payload.decode('utf-8', errors='replace'))
                    sys.stdout.flush()
            else:
                sys.stdout.write(strip_ansi_sequences(payload).decode('utf-8', errors='replace'))
                sys.stdout.flush()

    def _handle_output_packet(self, seq, payload):
        if seq == self.recv_expected_seq:
            self._process_output_payload(payload)
            self.recv_expected_seq = next_data_seq(self.recv_expected_seq)
            while self.recv_expected_seq in self.recv_buffer:
                buffered_payload = self.recv_buffer.pop(self.recv_expected_seq)
                self.recv_buffer_bytes -= len(buffered_payload)
                self._process_output_payload(buffered_payload)
                self.recv_expected_seq = next_data_seq(self.recv_expected_seq)
            return True
        if is_sequence_ahead(seq, self.recv_expected_seq):
            if seq in self.recv_buffer:
                return True
            if len(self.recv_buffer) >= self.recv_buffer_limit_packets:
                return False
            if self.recv_buffer_bytes + len(payload) > self.recv_buffer_limit_bytes:
                return False
            self.recv_buffer[seq] = payload
            self.recv_buffer_bytes += len(payload)
            return True
        return True

    def _recv_loop(self):
        while self.running:
            try:
                data, _ = self.sock.recvfrom(65535)
                msg = unpack_msg(data)
                if not msg:
                    continue
                msg_type, seq, _, payload = msg

                if msg_type == TYPE_ACK:
                    prompt_info = unpack_prompt_info(payload)
                    if prompt_info:
                        self._apply_prompt_info(*prompt_info)
                    if seq == HEARTBEAT_SEQ:
                        self.last_heartbeat_ack = time.monotonic()
                        self.heartbeat_ack_event.set()
                    if seq == self.waiting_seq:
                        self.ack_event.set()
                elif msg_type == TYPE_OUTPUT:
                    accepted = self._handle_output_packet(seq, payload)
                    if accepted:
                        ack_msg = pack_msg(TYPE_ACK, seq, self.client_id, self._make_window_update_payload())
                        self.sock.sendto(ack_msg, self.server_addr)
                    else:
                        self._send_window_update()
            except socket.timeout:
                continue
            except (ConnectionResetError, OSError):
                if self.running and self.connected:
                    self._handle_connection_error()
            except Exception as e:
                if self.running and self.connected:
                    print(f"\nRecv error: {e}")

    def _send_reliable(self, data):
        max_retries = 5
        retry_count = 0
        self.waiting_seq = self.send_seq
        self.ack_event.clear()

        while retry_count < max_retries and self.running:
            try:
                msg = pack_msg(TYPE_COMMAND, self.send_seq, self.client_id, data)
                self.sock.sendto(msg, self.server_addr)
                if self.ack_event.wait(timeout=2):
                    if not self.running:
                        break
                    self.send_seq = next_data_seq(self.send_seq)
                    self.waiting_seq = -1
                    return True
                retry_count += 1
                with self.print_lock:
                    print(f"\nTimeout waiting for ACK, retrying {retry_count}/{max_retries}")
            except Exception as e:
                if not self.running:
                    break
                with self.print_lock:
                    print(f"\nSend error: {e}")
                retry_count += 1

        self.waiting_seq = -1
        if self.running:
            with self.print_lock:
                print("\nSend failed after retries: server may be unreachable or network interrupted")
        return False

    def _get_terminal_size(self):
        size = shutil.get_terminal_size(fallback=(80, 24))
        return size.lines, size.columns

    def _send_resize(self, rows=None, cols=None):
        try:
            if rows is None or cols is None:
                rows, cols = self._get_terminal_size()
            rows = max(1, min(1000, int(rows)))
            cols = max(1, min(1000, int(cols)))
            self.last_rows = rows
            self.last_cols = cols
            msg = pack_msg(TYPE_RESIZE, self.resize_seq, self.client_id, pack_resize(rows, cols))
            self.sock.sendto(msg, self.server_addr)
            self.resize_seq = next_data_seq(self.resize_seq)
        except Exception:
            pass

    def _install_resize_handler(self):
        if os.name == 'nt' or not hasattr(signal, 'SIGWINCH'):
            return
        try:
            self.saved_sigwinch_handler = signal.getsignal(signal.SIGWINCH)

            def handle_winch(signum, frame):
                self._send_resize()

            signal.signal(signal.SIGWINCH, handle_winch)
        except (ValueError, AttributeError):
            pass

    def _restore_resize_handler(self):
        if os.name == 'nt' or self.saved_sigwinch_handler is None or not hasattr(signal, 'SIGWINCH'):
            return
        try:
            signal.signal(signal.SIGWINCH, self.saved_sigwinch_handler)
        except (ValueError, AttributeError):
            pass
        self.saved_sigwinch_handler = None

    def _poll_resize_if_changed(self):
        rows, cols = self._get_terminal_size()
        if rows != self.last_rows or cols != self.last_cols:
            self._send_resize(rows, cols)

    def _enter_raw_mode(self):
        if os.name == 'nt' or not sys.stdin.isatty() or self.raw_terminal_attrs is not None:
            return
        try:
            fd = sys.stdin.fileno()
            self.raw_terminal_attrs = termios.tcgetattr(fd)
            tty.setraw(fd)
        except Exception:
            self.raw_terminal_attrs = None

    def _restore_terminal_mode(self):
        if os.name == 'nt' or self.raw_terminal_attrs is None:
            return
        try:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self.raw_terminal_attrs)
        except Exception:
            pass
        self.raw_terminal_attrs = None

    def _send_stdin(self, data):
        for i in range(0, len(data), MAX_DATA_SIZE):
            chunk = data[i:i + MAX_DATA_SIZE]
            try:
                msg = pack_msg(TYPE_STDIN, self.stdin_seq, self.client_id, chunk)
                self.sock.sendto(msg, self.server_addr)
                self.stdin_seq = next_data_seq(self.stdin_seq)
            except Exception:
                return False
        return True

    def _get_windows_stdin_handle(self):
        if os.name != 'nt':
            return None
        try:
            handle = _kernel32.GetStdHandle(STD_INPUT_HANDLE)
        except Exception:
            return None
        if handle in (None, 0, INVALID_HANDLE_VALUE):
            return None
        return handle

    def _windows_special_key_to_bytes(self, code):
        mapping = {
            'H': b'\x1b[A',
            'P': b'\x1b[B',
            'M': b'\x1b[C',
            'K': b'\x1b[D',
            'G': b'\x1b[H',
            'O': b'\x1b[F',
            'S': b'\x1b[3~',
            'I': b'\x1b[5~',
            'Q': b'\x1b[6~',
            0x26: b'\x1b[A',
            0x28: b'\x1b[B',
            0x27: b'\x1b[C',
            0x25: b'\x1b[D',
            0x24: b'\x1b[H',
            0x23: b'\x1b[F',
            0x2E: b'\x1b[3~',
            0x21: b'\x1b[5~',
            0x22: b'\x1b[6~',
        }
        return mapping.get(code, b'')

    def _windows_char_to_input_bytes(self, ch):
        if ch == '\r':
            return b'\r'
        if ch == '\b':
            return b'\x7f'

        codepoint = ord(ch)
        pending = self.pending_windows_high_surrogate
        if pending is not None:
            self.pending_windows_high_surrogate = None
            high = ord(pending)
            if 0xDC00 <= codepoint <= 0xDFFF:
                combined = 0x10000 + ((high - 0xD800) << 10) + (codepoint - 0xDC00)
                return chr(combined).encode('utf-8')

        if 0xD800 <= codepoint <= 0xDBFF:
            self.pending_windows_high_surrogate = ch
            return b''
        if 0xDC00 <= codepoint <= 0xDFFF:
            return b''
        return ch.encode('utf-8', errors='ignore')

    def _read_windows_console_key(self):
        handle = self.windows_stdin_handle
        if handle is None:
            return None
        pending = wintypes.DWORD()
        try:
            if not _kernel32.GetNumberOfConsoleInputEvents(handle, ctypes.byref(pending)):
                return None
            if pending.value == 0:
                return None

            chunks = []
            record = _WindowsInputRecord()
            read = wintypes.DWORD()
            for _ in range(pending.value):
                if not _kernel32.ReadConsoleInputW(handle, ctypes.byref(record), 1, ctypes.byref(read)):
                    return None
                if read.value == 0:
                    break
                if record.EventType != KEY_EVENT:
                    continue
                event = record.Event.KeyEvent
                if not event.bKeyDown:
                    continue
                ch = event.uChar.UnicodeChar
                repeat = max(1, event.wRepeatCount)
                if ch and ch != '\x00':
                    data = self._windows_char_to_input_bytes(ch)
                else:
                    data = self._windows_special_key_to_bytes(event.wVirtualKeyCode)
                if data:
                    chunks.extend(data for _ in range(repeat))
            return b''.join(chunks)
        except Exception:
            return None

    def _read_windows_key(self):
        data = self._read_windows_console_key()
        if data is not None:
            return data
        if not msvcrt.kbhit():
            return None
        chunks = []
        while msvcrt.kbhit():
            ch = msvcrt.getwch()
            if ch in ('\x00', '\xe0'):
                if not msvcrt.kbhit():
                    continue
                code = msvcrt.getwch()
                data = self._windows_special_key_to_bytes(code)
            else:
                data = self._windows_char_to_input_bytes(ch)
            if data:
                chunks.append(data)
        return b''.join(chunks)

    def _run_interactive_until_done(self):
        self.interactive_mode = True
        self._enter_raw_mode()
        try:
            while self.running and not self.output_done_event.is_set():
                self._poll_resize_if_changed()
                try:
                    if os.name == 'nt':
                        data = self._read_windows_key()
                        if data:
                            self._send_stdin(data)
                        else:
                            self.output_done_event.wait(0.005)
                    elif sys.stdin.isatty():
                        readable, _, _ = select.select([sys.stdin], [], [], 0.05)
                        if readable:
                            data = os.read(sys.stdin.fileno(), 1024)
                            if data:
                                self._send_stdin(data)
                    else:
                        self.output_done_event.wait(0.05)
                except KeyboardInterrupt:
                    self._send_stdin(b'\x03')
        finally:
            self._restore_terminal_mode()
            self.interactive_mode = False


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='UDP Remote Terminal Client')
    parser.add_argument('host', nargs='?', default='127.0.0.1', help='Server host')
    parser.add_argument('port', nargs='?', type=int, default=9999, help='Server port')
    args = parser.parse_args()
    client = UDPClient(args.host, args.port)
    client.start()
