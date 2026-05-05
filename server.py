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
    def __init__(self, cmd, cwd, rows, cols):
        try:
            PtyProcess = importlib.import_module('winpty').PtyProcess
        except ImportError as exc:
            raise RuntimeError("Windows PTY requires pywinpty; install it with: pip install pywinpty") from exc

        last_error = None
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
        text = self.stdin_decoder.decode(data)
        if text:
            self.proc.write(text)

    def resize(self, rows, cols):
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
    def __init__(self, addr):
        self.addr = addr
        self.last_heartbeat = time.time()
        self.recv_expected_seq = 0
        self.send_seq = 0
        self.cwd = os.getcwd()
        self.current_process = None
        self.current_command = None
        self.process_lock = threading.Lock()
        self.send_lock = threading.Lock()
        self.advertised_window_packets = OUTPUT_WINDOW_SIZE
        self.advertised_window_bytes = RECV_BUFFER_LIMIT_BYTES
        self.window_lock = threading.Lock()
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
    def __init__(self, host='0.0.0.0', port=9999):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.clients = {}
        self.ack_events = {}
        self.ack_lock = threading.Lock()
        self.running = False
        self.encoding = locale.getpreferredencoding(False) or 'utf-8'
        self.server_user = getpass.getuser()
        self.server_host = platform.node() or socket.gethostname()
        print(f"UDP Server started on {host}:{port}")

    def start(self):
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
        self.running = False
        for client in list(self.clients.values()):
            self._cleanup_client_runtime(client)
        self.sock.close()
        print("Server stopped")

    def _recv_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1500)
                msg = unpack_msg(data)
                if not msg:
                    continue
                msg_type, seq, client_id, payload = msg

                if client_id not in self.clients:
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
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] Heartbeat received from client {client_id} at {addr}")
        client = self.clients[client_id]
        payload = pack_prompt_info(self.server_user, self.server_host, client.cwd)
        ack_msg = pack_msg(TYPE_ACK, seq, client_id, payload)
        self.sock.sendto(ack_msg, addr)

    def _handle_ack(self, client_id, seq, payload=b''):
        self._apply_window_update(client_id, payload)
        with self.ack_lock:
            key = (client_id, seq)
            if key in self.ack_events:
                self.ack_events[key].set()

    def _apply_window_update(self, client_id, payload_or_update):
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
        client = self.clients.get(client_id)
        if not client:
            return

        ack_msg = pack_msg(TYPE_ACK, seq, client_id, b'')
        self.sock.sendto(ack_msg, addr)

        expected = client.stdin_recv_expected_seq
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
        with client.pty_lock:
            pending = client.pending_stdin
            client.pending_stdin = []
            client.stdin_event.clear()
        if pending:
            self._write_pty(client, b''.join(pending))

    def _set_pty_size_fd(self, fd, rows, cols):
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
        if not chunks:
            return True

        client = self.clients.get(client_id)
        if not client:
            return False

        with client.send_lock:
            addr = client.addr
            packets = []
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

            base = 0
            next_to_send = 0
            last_progress = time.monotonic()
            try:
                while base < len(packets) and self.running:
                    for packet in packets[base:next_to_send]:
                        if not packet['acked'] and packet['event'].is_set():
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
                    while next_to_send < len(packets) and next_to_send - base < effective_window:
                        packet = packets[next_to_send]
                        self._send_packet(client_id, addr, packet['seq'], packet['chunk'])
                        packet['sent'] = True
                        packet['last_sent'] = time.monotonic()
                        next_to_send += 1
                        last_progress = time.monotonic()

                    now = time.monotonic()
                    timed_out = any(
                        not packet['acked'] and packet['sent'] and now - packet['last_sent'] >= ACK_TIMEOUT
                        for packet in packets[base:next_to_send]
                    )
                    if timed_out:
                        for packet in packets[base:next_to_send]:
                            if packet['acked']:
                                continue
                            if packet['retries'] >= MAX_RETRIES:
                                print(f"Failed to send to client {client_id} after {MAX_RETRIES} retries")
                                return False
                            packet['retries'] += 1
                            self._send_packet(client_id, addr, packet['seq'], packet['chunk'])
                            packet['last_sent'] = time.monotonic()
                            last_progress = time.monotonic()
                            print(f"Retry {packet['retries']}/{MAX_RETRIES} for client {client_id} seq {packet['seq']}")
                        continue

                    if next_to_send == base and effective_window <= 0:
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
        chunks = [data[i:i + MAX_DATA_SIZE] for i in range(0, len(data), MAX_DATA_SIZE)]
        return self._send_output_chunks_reliable(client_id, chunks)

    def _send_output_done(self, client_id):
        client = self.clients.get(client_id)
        if not client:
            return False
        return self._send_output_chunks_reliable(client_id, [pack_output_done(client.cwd)])

    def _send_reliable(self, client_id, data):
        chunks = [data[i:i + MAX_DATA_SIZE] for i in range(0, len(data), MAX_DATA_SIZE)]
        chunks.append(pack_output_done(self.clients[client_id].cwd))
        return self._send_output_chunks_reliable(client_id, chunks)

    def _handle_command(self, client_id, seq, payload, addr):
        client = self.clients[client_id]

        if seq != client.recv_expected_seq:
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
        try:
            return shlex.split(cmd, posix=os.name != 'nt')
        except ValueError:
            return cmd.split()

    def _is_cd_command(self, cmd):
        tokens = self._split_command(cmd)
        return bool(tokens) and tokens[0].lower() == 'cd' and not any(token in {'&', '&&', '|', '||', ';'} for token in tokens)

    def _is_ping_command(self, cmd):
        tokens = self._split_command(cmd or '')
        if not tokens:
            return False
        exe = os.path.basename(tokens[0]).lower()
        if exe.endswith('.exe'):
            exe = exe[:-4]
        return exe == 'ping'

    def _change_directory_and_respond(self, client_id, cmd):
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

    def _stream_pipe(self, client_id, pipe, cmd=None):
        try:
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
                    text, interrupted = self._normalize_interrupt_text(text, suppress_after_interrupt)
                    if interrupted:
                        if text:
                            self._send_output_reliable(client_id, text.encode('utf-8'))
                        break
                if text and not self._send_output_reliable(client_id, text.encode('utf-8')):
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
            if tail:
                self._send_output_reliable(client_id, tail.encode('utf-8'))
        except Exception as e:
            if self.running:
                print(f"Stream error: {e}")
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    def _execute_and_respond(self, client_id, cmd):
        client = self.clients.get(client_id)
        if not client:
            return

        proc = None
        reader_threads = []
        try:
            if os.name == 'nt':
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=client.cwd,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                    shell=True
                )
            else:
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
                if pipe is None:
                    continue
                thread = threading.Thread(target=self._stream_pipe, args=(client_id, pipe, cmd), daemon=True)
                thread.start()
                reader_threads.append(thread)

            proc.wait()

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
        cmd = strip_pty_prefix(cmd)
        if not cmd:
            self._send_reliable(client_id, b"Error: empty PTY command\n")
            return
        if os.name == 'nt':
            self._execute_windows_pty_and_respond(client_id, cmd)
        else:
            self._execute_posix_pty_and_respond(client_id, cmd)

    def _execute_posix_pty_and_respond(self, client_id, cmd):
        client = self.clients.get(client_id)
        if not client:
            return

        master_fd = None
        slave_fd = None
        proc = None
        try:
            master_fd, slave_fd = pty.openpty()
            self._set_pty_size_fd(slave_fd, client.term_rows, client.term_cols)
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
            with client.pty_lock:
                client.pty_fd = master_fd
                client.pty_session = None
                client.is_interactive = True
                client.stdin_recv_expected_seq = 0
            self._flush_pending_stdin(client)

            while self.running and proc.poll() is None:
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
        client = self.clients.get(client_id)
        if not client:
            return

        session = None
        try:
            session = WindowsPtySession(cmd, client.cwd, client.term_rows, client.term_cols)
            with client.pty_lock:
                client.pty_session = session
                client.pty_fd = None
                client.is_interactive = True
                client.stdin_recv_expected_seq = 0
            self._flush_pending_stdin(client)

            while self.running and session.is_alive():
                self._flush_pending_stdin(client)
                chunk = session.read(4096)
                if chunk:
                    chunk = self._read_available_windows_pty_output(session, chunk)
                    if not self._send_output_reliable(client_id, chunk):
                        break
                elif not session.is_alive():
                    break

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
