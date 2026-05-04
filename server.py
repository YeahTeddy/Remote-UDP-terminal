import socket
import threading
import time
import subprocess
import os
import locale
from common import *


class ClientInfo:
    def __init__(self, addr):
        self.addr = addr
        self.last_heartbeat = time.time()
        self.recv_expected_seq = 0
        self.send_seq = 0
        self.current_process = None
        self.process_lock = threading.Lock()


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
        print(f"Heartbeat received from client {client_id} at {addr}")
        ack_msg = pack_msg(TYPE_ACK, seq, client_id, b'')
        self.sock.sendto(ack_msg, addr)

    def _handle_ack(self, client_id, seq):
        with self.ack_lock:
            key = (client_id, seq)
            if key in self.ack_events:
                self.ack_events[key].set()

    def _handle_interrupt(self, client_id):
        client = self.clients.get(client_id)
        if client:
            with client.process_lock:
                if client.current_process:
                    try:
                        client.current_process.terminate()
                    except Exception:
                        pass
                    client.current_process = None

    def _send_reliable(self, client_id, data):
        client = self.clients.get(client_id)
        if not client:
            return False
        addr = client.addr
        max_retries = 5

        chunks = [data[i:i + MAX_DATA_SIZE] for i in range(0, len(data), MAX_DATA_SIZE)]
        chunks.append(b'')

        for chunk in chunks:
            seq = client.send_seq
            ack_event = threading.Event()
            with self.ack_lock:
                self.ack_events[(client_id, seq)] = ack_event

            retry_count = 0
            success = False
            while retry_count < max_retries and self.running:
                try:
                    msg = pack_msg(TYPE_OUTPUT, seq, client_id, chunk)
                    self.sock.sendto(msg, addr)
                    if ack_event.wait(timeout=2):
                        success = True
                        break
                    retry_count += 1
                    print(f"Retry {retry_count}/{max_retries} for client {client_id} seq {seq}")
                except Exception as e:
                    print(f"Send error: {e}")
                    retry_count += 1

            with self.ack_lock:
                self.ack_events.pop((client_id, seq), None)

            if not success:
                print(f"Failed to send to client {client_id} after {max_retries} retries")
                return False

            client.send_seq = next_data_seq(client.send_seq)

        return True

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

        threading.Thread(
            target=self._execute_and_respond,
            args=(client_id, cmd),
            daemon=True
        ).start()

    def _decode_output(self, data):
        if not data:
            return ''
        return data.decode(self.encoding, errors='replace')

    def _execute_and_respond(self, client_id, cmd):
        client = self.clients.get(client_id)
        if not client:
            return

        try:
            if os.name == 'nt':
                proc = subprocess.Popen(
                    ['cmd.exe', '/c', cmd],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )
            else:
                proc = subprocess.Popen(
                    ['bash', '-c', cmd],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    preexec_fn=os.setsid
                )

            with client.process_lock:
                client.current_process = proc

            try:
                stdout, stderr = proc.communicate(timeout=30)
                output = self._decode_output(stdout) + self._decode_output(stderr)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
                output = self._decode_output(stdout) + self._decode_output(stderr) + "Error: Command execution timed out\n"
            finally:
                with client.process_lock:
                    if client.current_process == proc:
                        client.current_process = None
        except Exception as e:
            output = f"Error: {str(e)}\n"

        if client_id in self.clients:
            self._send_reliable(client_id, output.encode('utf-8'))

    def _cleanup_loop(self):
        while self.running:
            now = time.time()
            to_remove = []
            for cid, client in list(self.clients.items()):
                if now - client.last_heartbeat > 30:
                    with client.process_lock:
                        if client.current_process:
                            try:
                                client.current_process.terminate()
                            except Exception:
                                pass
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
