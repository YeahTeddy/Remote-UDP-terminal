import socket
import threading
import time
import random
import signal
import sys
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
        self.ack_event = threading.Event()
        self.output_done_event = threading.Event()
        self.waiting_seq = -1
        self.print_lock = threading.Lock()
        self.prompt_ready_event = threading.Event()
        self.prompt_user = 'user'
        self.prompt_host = server_host
        self.prompt_dir = '~'
        self.prompt_interrupt_seen = False
        self.suppress_interrupt_until = 0
        self.saved_sigint_handler = None
        self.resume_sigint_at = 0
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
                if self._is_suppressed_interrupt():
                    continue
                self._print_interrupt_marker()
                self._send_interrupt()
                return True
        return False

    def start(self):
        self.running = True
        recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        recv_thread.start()
        self._send_heartbeat()
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        print("Connected to server, enter commands to execute, 'exit' to quit")
        self._wait_for_prompt_info()

        while self.running:
            try:
                self._resume_sigint()
                cmd = input(self._prompt())
            except KeyboardInterrupt:
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

            self.output_done_event.clear()
            self._begin_command_interrupt_window()
            sent = self._send_command_reliable((cmd + '\n').encode('utf-8'))
            if sent:
                while self.running and not self.output_done_event.is_set():
                    try:
                        self.output_done_event.wait(0.1)
                    except KeyboardInterrupt:
                        if self._is_suppressed_interrupt():
                            continue
                        self._print_interrupt_marker()
                        self._send_interrupt()

    def stop(self):
        if not self.running:
            return
        self._resume_sigint()
        self.running = False
        try:
            self.sock.close()
        except OSError:
            pass
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
        except Exception:
            pass

    def _heartbeat_loop(self):
        while self.running:
            time.sleep(5)
            self._send_heartbeat()

    def _wait_for_prompt_info(self):
        try:
            self.prompt_ready_event.wait(timeout=2)
        except KeyboardInterrupt:
            self._handle_prompt_interrupt()

    def _apply_prompt_info(self, user, host, cwd):
        if user:
            self.prompt_user = user
        if host:
            self.prompt_host = host
        if cwd:
            self.prompt_dir = self._format_prompt_dir(cwd)
        self.prompt_ready_event.set()

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
                    if seq == self.waiting_seq:
                        self.ack_event.set()
                elif msg_type == TYPE_OUTPUT:
                    if seq == self.recv_expected_seq:
                        done_cwd = unpack_output_done(payload)
                        if done_cwd is not None:
                            if done_cwd:
                                self.prompt_dir = self._format_prompt_dir(done_cwd)
                            self.output_done_event.set()
                        else:
                            with self.print_lock:
                                sys.stdout.write(payload.decode('utf-8', errors='replace'))
                                sys.stdout.flush()
                        self.recv_expected_seq = next_data_seq(self.recv_expected_seq)
                    ack_msg = pack_msg(TYPE_ACK, seq, self.client_id, b'')
                    self.sock.sendto(ack_msg, self.server_addr)
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
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
                    self.send_seq = next_data_seq(self.send_seq)
                    self.waiting_seq = -1
                    return True
                retry_count += 1
                with self.print_lock:
                    print(f"\nTimeout waiting for ACK, retrying {retry_count}/{max_retries}")
            except Exception as e:
                with self.print_lock:
                    print(f"\nSend error: {e}")
                retry_count += 1

        self.waiting_seq = -1
        with self.print_lock:
            print("\nSend failed after retries: server may be unreachable or network interrupted")
        return False


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='UDP Remote Terminal Client')
    parser.add_argument('host', nargs='?', default='127.0.0.1', help='Server host')
    parser.add_argument('port', nargs='?', type=int, default=9999, help='Server port')
    args = parser.parse_args()
    client = UDPClient(args.host, args.port)
    client.start()
