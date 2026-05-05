import codecs
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
from common import *


class ClientInfo:
    def __init__(self, addr):
        self.addr = addr
        self.last_heartbeat = time.time()
        self.recv_expected_seq = 0
        self.send_seq = 0
        self.cwd = os.getcwd()
        self.current_process = None
        self.process_lock = threading.Lock()
        self.send_lock = threading.Lock()


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
                    self._handle_ack(client_id, seq)
                elif msg_type == TYPE_COMMAND:
                    self._handle_command(client_id, seq, payload, addr)
                elif msg_type == TYPE_INTERRUPT:
                    self._handle_interrupt(client_id)
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

    def _handle_ack(self, client_id, seq):
        with self.ack_lock:
            key = (client_id, seq)
            if key in self.ack_events:
                self.ack_events[key].set()

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

    def _interrupt_process(self, proc):
        try:
            if os.name == 'nt':
                proc.send_signal(signal.CTRL_BREAK_EVENT)
                time.sleep(0.2)
                if proc.poll() is None:
                    self._kill_process_tree(proc)
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        except Exception:
            self._kill_process_tree(proc)

    def _handle_interrupt(self, client_id):
        client = self.clients.get(client_id)
        if not client:
            return

        with client.process_lock:
            proc = client.current_process

        if proc and proc.poll() is None:
            self._interrupt_process(proc)

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
            try:
                while base < len(packets) and self.running:
                    while next_to_send < len(packets) and next_to_send - base < OUTPUT_WINDOW_SIZE:
                        packet = packets[next_to_send]
                        self._send_packet(client_id, addr, packet['seq'], packet['chunk'])
                        packet['sent'] = True
                        packet['last_sent'] = time.monotonic()
                        next_to_send += 1

                    for packet in packets[base:next_to_send]:
                        if not packet['acked'] and packet['event'].is_set():
                            packet['acked'] = True

                    while base < len(packets) and packets[base]['acked']:
                        with self.ack_lock:
                            self.ack_events.pop((client_id, packets[base]['seq']), None)
                        base += 1

                    if base >= len(packets):
                        return True

                    now = time.monotonic()
                    timed_out = any(
                        not packet['acked'] and packet['sent'] and now - packet['last_sent'] >= ACK_TIMEOUT
                        for packet in packets[base:next_to_send]
                    )
                    if not timed_out:
                        time.sleep(0.01)
                        continue

                    for packet in packets[base:next_to_send]:
                        if packet['acked']:
                            continue
                        if packet['retries'] >= MAX_RETRIES:
                            print(f"Failed to send to client {client_id} after {MAX_RETRIES} retries")
                            return False
                        packet['retries'] += 1
                        self._send_packet(client_id, addr, packet['seq'], packet['chunk'])
                        packet['last_sent'] = time.monotonic()
                        print(f"Retry {packet['retries']}/{MAX_RETRIES} for client {client_id} seq {packet['seq']}")
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
        client = self.clients.get(client_id)
        if not client:
            return False
        chunks = [data[i:i + MAX_DATA_SIZE] for i in range(0, len(data), MAX_DATA_SIZE)]
        chunks.append(pack_output_done(client.cwd))
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

        threading.Thread(
            target=self._execute_and_respond,
            args=(client_id, cmd),
            daemon=True
        ).start()

    def _split_command(self, cmd):
        try:
            return shlex.split(cmd, posix=os.name != 'nt')
        except ValueError:
            return cmd.split()

    def _is_cd_command(self, cmd):
        tokens = self._split_command(cmd)
        return bool(tokens) and tokens[0].lower() == 'cd' and not any(token in {'&', '&&', '|', '||', ';'} for token in tokens)

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

    def _stream_pipe(self, client_id, pipe):
        try:
            utf8_decoder = codecs.getincrementaldecoder('utf-8')(errors='strict')
            local_decoder = codecs.getincrementaldecoder(self.encoding)(errors='replace')
            use_utf8 = True
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
        timed_out = False
        try:
            if os.name == 'nt':
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
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

            for pipe in (proc.stdout, proc.stderr):
                if pipe is None:
                    continue
                thread = threading.Thread(target=self._stream_pipe, args=(client_id, pipe), daemon=True)
                thread.start()
                reader_threads.append(thread)

            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                timed_out = True
                self._kill_process_tree(proc)
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass

            for thread in reader_threads:
                thread.join()

            if timed_out:
                self._send_output_reliable(client_id, b"Error: Command execution timed out\n")
        except Exception as e:
            self._send_output_reliable(client_id, f"Error: {str(e)}\n".encode('utf-8'))
        finally:
            if proc is not None:
                with client.process_lock:
                    if client.current_process == proc:
                        client.current_process = None
            if client_id in self.clients:
                self._send_output_done(client_id)

    def _cleanup_loop(self):
        while self.running:
            now = time.time()
            to_remove = []
            for cid, client in list(self.clients.items()):
                if now - client.last_heartbeat > 30:
                    with client.process_lock:
                        proc = client.current_process
                    if proc and proc.poll() is None:
                        self._kill_process_tree(proc)
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
