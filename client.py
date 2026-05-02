import socket
import threading
import time
import random
import sys
import os
from common import *

HEARTBEAT_SEQ = 255

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
        self.waiting_seq = -1
        self.print_lock = threading.Lock()
        print(f"Client ID: {self.client_id}, connecting to {server_host}:{server_port}")

    def _input_thread(self):
        while self.running:
            try:
                cmd = sys.stdin.readline()
                if not cmd:
                    continue
                if cmd.strip() == 'exit':
                    self.stop()
                    break
                self._send_reliable(cmd.encode('utf-8'))
            except:
                break

    def start(self):
        self.running = True
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        recv_thread.start()
        input_thread = threading.Thread(target=self._input_thread, daemon=True)
        input_thread.start()
        print("Connected to server, enter commands to execute, 'exit' to quit")
        try:
            while self.running:
                time.sleep(0.1)
        except KeyboardInterrupt:
            self._send_interrupt()
            time.sleep(0.5)
            self.stop()

    def stop(self):
        self.running = False
        self.sock.close()
        print("\nClient stopped")

    def _send_interrupt(self):
        try:
            msg = pack_msg(TYPE_INTERRUPT, self.send_seq, self.client_id, b'')
            self.sock.sendto(msg, self.server_addr)
        except:
            pass

    def _heartbeat_loop(self):
        while self.running:
            try:
                msg = pack_msg(TYPE_HEARTBEAT, HEARTBEAT_SEQ, self.client_id, b'')
                self.sock.sendto(msg, self.server_addr)
            except:
                pass
            time.sleep(5)

    def _recv_loop(self):
        while self.running:
            try:
                data, _ = self.sock.recvfrom(65535)
                msg = unpack_msg(data)
                if not msg:
                    continue
                msg_type, seq, _, payload = msg

                if msg_type == TYPE_ACK:
                    if seq == self.waiting_seq:
                        self.ack_event.set()
                elif msg_type == TYPE_DATA:
                    if seq == self.recv_expected_seq:
                        with self.print_lock:
                            sys.stdout.write(payload.decode('utf-8', errors='replace'))
                            sys.stdout.flush()
                        self.recv_expected_seq = (self.recv_expected_seq + 1) % 256
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
                msg = pack_msg(TYPE_DATA, self.send_seq, self.client_id, data)
                self.sock.sendto(msg, self.server_addr)
                if self.ack_event.wait(timeout=2):
                    self.send_seq = (self.send_seq + 1) % 256
                    self.waiting_seq = -1
                    return True
                else:
                    retry_count += 1
                    with self.print_lock:
                        print(f"\nTimeout, retrying {retry_count}/{max_retries}")
            except Exception as e:
                with self.print_lock:
                    print(f"\nSend error: {e}")
                retry_count += 1

        self.waiting_seq = -1
        with self.print_lock:
            print("\nSend failed after max retries, server may be unreachable")
        return False

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='UDP Remote Terminal Client')
    parser.add_argument('host', nargs='?', default='127.0.0.1', help='Server host')
    parser.add_argument('port', nargs='?', type=int, default=9999, help='Server port')
    args = parser.parse_args()
    client = UDPClient(args.host, args.port)
    client.start()
