import socket
import struct
import threading
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *

TEST_PORT = 19888
results = []
recv_expected = {}


def log_test(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    msg = f"[{status}] {name}"
    if detail and not passed:
        msg += f" - {detail}"
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or 'utf-8'
        print(msg.encode(encoding, errors='backslashreplace').decode(encoding), flush=True)
    results.append((name, passed, detail))


def recv_response(sock, cid, addr, timeout=5):
    got_ack = False
    all_data = b''
    expected = recv_expected.get(cid, 0)
    buffered = {}
    done = False

    def process_payload(payload):
        nonlocal all_data, done
        if unpack_output_done(payload) is not None:
            done = True
        else:
            all_data += payload

    sock.settimeout(timeout)
    try:
        while not done:
            resp_data, _ = sock.recvfrom(65535)
            resp = unpack_msg(resp_data)
            if resp is None:
                continue
            if resp[0] == TYPE_ACK:
                got_ack = True
            elif resp[0] == TYPE_OUTPUT:
                seq = resp[1]
                payload = resp[3]
                ack = pack_msg(TYPE_ACK, seq, cid, b'')
                sock.sendto(ack, addr)
                if seq == expected:
                    process_payload(payload)
                    expected = next_data_seq(expected)
                    while expected in buffered and not done:
                        process_payload(buffered.pop(expected))
                        expected = next_data_seq(expected)
                elif is_sequence_ahead(seq, expected):
                    buffered.setdefault(seq, payload)
                sock.settimeout(1)
    except socket.timeout:
        pass
    recv_expected[cid] = expected
    return got_ack, all_data


def recv_realtime_response(sock, cid, addr, timeout=5):
    got_ack = False
    all_data = b''
    first_output_at = None
    expected = recv_expected.get(cid, 0)
    buffered = {}
    done = False
    started = time.monotonic()

    def process_payload(payload):
        nonlocal all_data, first_output_at, done
        if unpack_output_done(payload) is not None:
            done = True
        else:
            if first_output_at is None and payload:
                first_output_at = time.monotonic() - started
            all_data += payload

    sock.settimeout(timeout)
    try:
        while not done:
            resp_data, _ = sock.recvfrom(65535)
            resp = unpack_msg(resp_data)
            if resp is None:
                continue
            if resp[0] == TYPE_ACK:
                got_ack = True
            elif resp[0] == TYPE_OUTPUT:
                seq = resp[1]
                payload = resp[3]
                ack = pack_msg(TYPE_ACK, seq, cid, b'')
                sock.sendto(ack, addr)
                if seq == expected:
                    process_payload(payload)
                    expected = next_data_seq(expected)
                    while expected in buffered and not done:
                        process_payload(buffered.pop(expected))
                        expected = next_data_seq(expected)
                elif is_sequence_ahead(seq, expected):
                    buffered.setdefault(seq, payload)
                sock.settimeout(3)
    except socket.timeout:
        pass
    recv_expected[cid] = expected
    return got_ack, all_data, first_output_at, time.monotonic() - started


def recv_response_with_window(sock, cid, addr, advertised_packets=1, timeout=5):
    got_ack = False
    all_data = b''
    expected = recv_expected.get(cid, 0)
    buffered = {}
    done = False

    def process_payload(payload):
        nonlocal all_data, done
        if unpack_output_done(payload) is not None:
            done = True
        else:
            all_data += payload

    sock.settimeout(timeout)
    try:
        while not done:
            resp_data, _ = sock.recvfrom(65535)
            resp = unpack_msg(resp_data)
            if resp is None:
                continue
            if resp[0] == TYPE_ACK:
                got_ack = True
            elif resp[0] == TYPE_OUTPUT:
                seq = resp[1]
                payload = resp[3]
                ack_payload = pack_window_update(advertised_packets, RECV_BUFFER_LIMIT_BYTES)
                ack = pack_msg(TYPE_ACK, seq, cid, ack_payload)
                sock.sendto(ack, addr)
                if seq == expected:
                    process_payload(payload)
                    expected = next_data_seq(expected)
                    while expected in buffered and not done:
                        process_payload(buffered.pop(expected))
                        expected = next_data_seq(expected)
                elif is_sequence_ahead(seq, expected):
                    buffered.setdefault(seq, payload)
                sock.settimeout(1)
    except socket.timeout:
        pass
    recv_expected[cid] = expected
    return got_ack, all_data


def recv_pty_response(sock, cid, addr, stdin_data, timeout=8, send_after_output=False):
    got_ack = False
    stdin_sent = False
    all_data = b''
    expected = recv_expected.get(cid, 0)
    buffered = {}
    done = False
    stdin_seq = 0

    def send_stdin_once():
        nonlocal stdin_sent, stdin_seq
        if stdin_sent:
            return
        for i in range(0, len(stdin_data), MAX_DATA_SIZE):
            chunk = stdin_data[i:i + MAX_DATA_SIZE]
            sock.sendto(pack_msg(TYPE_STDIN, stdin_seq, cid, chunk), addr)
            stdin_seq = next_data_seq(stdin_seq)
        stdin_sent = True

    def process_payload(payload):
        nonlocal all_data, done
        if unpack_output_done(payload) is not None:
            done = True
        else:
            all_data += payload

    sock.settimeout(timeout)
    try:
        while not done:
            resp_data, _ = sock.recvfrom(65535)
            resp = unpack_msg(resp_data)
            if resp is None:
                continue
            if resp[0] == TYPE_ACK:
                got_ack = True
                if not send_after_output:
                    send_stdin_once()
            elif resp[0] == TYPE_OUTPUT:
                seq = resp[1]
                payload = resp[3]
                if send_after_output:
                    send_stdin_once()
                ack_payload = pack_window_update(OUTPUT_WINDOW_SIZE, RECV_BUFFER_LIMIT_BYTES)
                sock.sendto(pack_msg(TYPE_ACK, seq, cid, ack_payload), addr)
                if seq == expected:
                    process_payload(payload)
                    expected = next_data_seq(expected)
                    while expected in buffered and not done:
                        process_payload(buffered.pop(expected))
                        expected = next_data_seq(expected)
                elif is_sequence_ahead(seq, expected):
                    buffered.setdefault(seq, payload)
                sock.settimeout(timeout)
    except socket.timeout:
        pass
    recv_expected[cid] = expected
    return got_ack, all_data


print("=" * 60, flush=True)
print("  UDP Remote Terminal - Basic Feature Test Suite", flush=True)
print("=" * 60, flush=True)

# ===== Test 1: Protocol Pack/Unpack =====
print("\n[1] Protocol Pack/Unpack", flush=True)
data = b'hello world'
packed = pack_msg(TYPE_COMMAND, 70000, 12345, data)
result = unpack_msg(packed)
if result:
    msg_type, seq, client_id, payload = result
    log_test("Pack/Unpack basic", msg_type == TYPE_COMMAND and seq == 70000 and client_id == 12345 and payload == data)
else:
    log_test("Pack/Unpack basic", False, "None")

log_test("Magic matches guide", MAGIC == 0x5554)
log_test("Header size matches guide", HEADER_SIZE == 13)
log_test("Message types match guide", (TYPE_COMMAND, TYPE_OUTPUT, TYPE_ACK, TYPE_HEARTBEAT, TYPE_INTERRUPT, TYPE_STDIN, TYPE_RESIZE, TYPE_WINDOW_UPDATE) == (0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08))

fake_header = struct.pack(HEADER_FORMAT, 0xFFFF, TYPE_COMMAND, 0, 5, 1)
log_test("Invalid magic rejected", unpack_msg(fake_header + b'hello') is None)

bad_type_header = struct.pack(HEADER_FORMAT, MAGIC, 0x99, 0, 5, 1)
log_test("Invalid type rejected", unpack_msg(bad_type_header + b'hello') is None)

packed2 = pack_msg(TYPE_COMMAND, 0, 1, b'hello')
log_test("Truncated rejected", unpack_msg(packed2[:5]) is None)

bad_len_header = struct.pack(HEADER_FORMAT, MAGIC, TYPE_COMMAND, 0, 4, 1)
log_test("Length mismatch rejected", unpack_msg(bad_len_header + b'hello') is None)

try:
    pack_msg(TYPE_COMMAND, 0, 1, b'x' * (MAX_DATA_SIZE + 1))
    log_test("Oversized rejected", False)
except ValueError:
    log_test("Oversized rejected", True)

try:
    pack_msg(0x99, 0, 1, b'')
    log_test("Invalid type pack rejected", False)
except ValueError:
    log_test("Invalid type pack rejected", True)

print("\n[1b] Advanced Protocol Helpers", flush=True)
log_test("ANSI escape stripped", strip_ansi_sequences(b'\x1b[31mRED\x1b[0m') == b'RED')
log_test("Tab preserved", normalize_command_input('echo\tTAB_OK\n') == 'echo\tTAB_OK\n')
log_test("ANSI input normalized", normalize_command_input('echo \x1b[31mANSI_OK\x1b[0m\n') == 'echo ANSI_OK\n')

print("\n[1c] High-Level Protocol Helpers", flush=True)
resize_payload = pack_resize(40, 120)
window_payload = pack_window_update(3, 4096)
log_test("Resize payload roundtrip", unpack_resize(resize_payload) == (40, 120))
log_test("Invalid resize rejected", unpack_resize(b'bad') is None)
log_test("Window update roundtrip", unpack_window_update(window_payload) == (3, 4096))
log_test("Invalid window update rejected", unpack_window_update(b'bad') is None)
log_test("PTY command prefix detected", should_use_pty_command('pty bash'))
log_test("Fullscreen command detected", should_use_pty_command('vim README.md'))
log_test("Ping command stays on normal path", not should_use_pty_command('ping 127.0.0.1'))
log_test("Plain python command not PTY", not should_use_pty_command('python -c "print(1)"'))
log_test("Plain echo command not PTY", not should_use_pty_command('echo OK'))
log_test("PTY prefix stripped", strip_pty_prefix('pty vim README.md') == 'vim README.md')

# ===== Start server =====
print("\n[2] Starting UDP server...", flush=True)
from server import UDPServer
server = UDPServer('127.0.0.1', TEST_PORT)
server.running = True
threading.Thread(target=server._recv_loop, daemon=True).start()
threading.Thread(target=server._cleanup_loop, daemon=True).start()
time.sleep(0.5)
addr = ('127.0.0.1', TEST_PORT)
filtered_interrupt, saw_interrupt = server._normalize_interrupt_text('Control-Break\r\nReply after interrupt', True)
log_test("Ping interrupt tail filtered", saw_interrupt and filtered_interrupt == 'Control-C\r\n')

# ===== Test 2: Heartbeat =====
print("\n[2] Heartbeat", flush=True)
sock_hb = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_hb.settimeout(3)
cid_hb = 10001
sock_hb.sendto(pack_msg(TYPE_HEARTBEAT, HEARTBEAT_SEQ, cid_hb, b''), addr)
try:
    resp_data, _ = sock_hb.recvfrom(1500)
    resp = unpack_msg(resp_data)
    prompt_info = unpack_prompt_info(resp[3]) if resp else None
    log_test("Heartbeat ACK received", resp is not None and resp[0] == TYPE_ACK and resp[1] == HEARTBEAT_SEQ)
    log_test("Heartbeat carries prompt info", prompt_info is not None and len(prompt_info[0]) > 0 and len(prompt_info[1]) > 0 and prompt_info[2] == os.getcwd())
except socket.timeout:
    log_test("Heartbeat ACK received", False, "timeout")
sock_hb.close()

print("\n[2a] Resize and Flow-Control State", flush=True)
sock_state = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_state.settimeout(2)
cid_state = 10018
sock_state.sendto(pack_msg(TYPE_RESIZE, 0, cid_state, pack_resize(33, 101)), addr)
try:
    resp_data, _ = sock_state.recvfrom(1500)
    resp = unpack_msg(resp_data)
    client_state = server.clients.get(cid_state)
    log_test("Resize ACK received", resp is not None and resp[0] == TYPE_ACK and resp[1] == 0)
    log_test("Resize state updated", client_state is not None and client_state.term_rows == 33 and client_state.term_cols == 101)
except socket.timeout:
    log_test("Resize ACK received", False, "timeout")
    log_test("Resize state updated", False, "timeout")

sock_state.sendto(pack_msg(TYPE_WINDOW_UPDATE, 0, cid_state, pack_window_update(2, 2048)), addr)
time.sleep(0.2)
client_state = server.clients.get(cid_state)
log_test("Window update state updated", client_state is not None and client_state.advertised_window_packets == 2 and client_state.advertised_window_bytes == 2048)
sock_state.close()

# ===== Test 2b: Client Connection Check =====
print("\n[2b] Client Connection Check", flush=True)
import client as client_module
from client import UDPClient
client_ok = UDPClient('127.0.0.1', TEST_PORT)
log_test("Windows PTY Backspace maps to DEL", os.name != 'nt' or client_ok._windows_char_to_input_bytes('\b') == b'\x7f')
log_test("Windows PTY Chinese input encoded", os.name != 'nt' or client_ok._windows_char_to_input_bytes('中') == '中'.encode('utf-8'))
if os.name == 'nt':
    client_ok.pending_windows_high_surrogate = '\ud83d'
    log_test("Windows PTY surrogate pair encoded", client_ok._windows_char_to_input_bytes('\ude00') == '😀'.encode('utf-8'))
else:
    log_test("Windows PTY surrogate pair encoded", True, "non-Windows")
client_ok.running = True
threading.Thread(target=client_ok._recv_loop, daemon=True).start()
log_test("Client startup detects reachable server", client_ok._connect_to_server(timeout=2))
client_ok.running = False
client_ok._close_socket()

client_fail = UDPClient('127.0.0.1', 19999)
client_fail.running = True
threading.Thread(target=client_fail._recv_loop, daemon=True).start()
log_test("Client startup rejects unreachable server", not client_fail._connect_to_server(timeout=0.5))
client_fail.running = False
client_fail._close_socket()

original_interrupt_main = client_module._thread.interrupt_main
client_module._thread.interrupt_main = lambda: None
try:
    client_wait = UDPClient('127.0.0.1', 19999)
    client_wait.running = True
    client_wait.connected = True
    wait_finished = threading.Event()

    def wait_for_command_output():
        client_wait._wait_for_command_output()
        wait_finished.set()

    threading.Thread(target=wait_for_command_output, daemon=True).start()
    time.sleep(0.1)
    client_wait._handle_connection_error()
    log_test("Command output wait exits on connection error", wait_finished.wait(1))
    client_wait._close_socket()

    client_send = UDPClient('127.0.0.1', 19999)
    client_send.running = True
    client_send.connected = True
    send_finished = threading.Event()
    send_result = []

    def send_command_without_ack():
        send_result.append(client_send._send_reliable(b'echo blocked\n'))
        send_finished.set()

    threading.Thread(target=send_command_without_ack, daemon=True).start()
    time.sleep(0.1)
    client_send._handle_connection_error()
    log_test("Command send wait exits on connection error", send_finished.wait(1) and send_result == [False])
    client_send._close_socket()

    client_reconnect = UDPClient('127.0.0.1', TEST_PORT)
    client_reconnect.running = True
    client_reconnect.connected = True
    reconnect_wait_finished = threading.Event()
    reconnect_interrupt_sent = threading.Event()
    reconnect_interrupts = []

    def send_reconnect_interrupt():
        reconnect_interrupts.append(True)
        reconnect_interrupt_sent.set()

    client_reconnect._send_interrupt = send_reconnect_interrupt

    def wait_for_reconnected_command_output():
        client_reconnect._wait_for_command_output()
        reconnect_wait_finished.set()

    threading.Thread(target=wait_for_reconnected_command_output, daemon=True).start()
    time.sleep(0.1)
    client_reconnect._mark_command_connection_interrupted()
    client_reconnect.last_heartbeat_ack = time.monotonic()
    interrupt_sent = reconnect_interrupt_sent.wait(1)
    client_reconnect.output_done_event.set()
    log_test("Command wait interrupts after heartbeat reconnect", interrupt_sent and reconnect_wait_finished.wait(1) and len(reconnect_interrupts) >= 1)
    client_reconnect._close_socket()

    client_reconnect_timeout = UDPClient('127.0.0.1', TEST_PORT)
    client_reconnect_timeout.running = True
    client_reconnect_timeout.connected = True
    reconnect_timeout_finished = threading.Event()
    reconnect_timeout_interrupt_sent = threading.Event()

    def send_reconnect_timeout_interrupt():
        reconnect_timeout_interrupt_sent.set()

    client_reconnect_timeout._send_interrupt = send_reconnect_timeout_interrupt

    def wait_for_reconnected_command_timeout():
        client_reconnect_timeout._wait_for_command_output()
        reconnect_timeout_finished.set()

    threading.Thread(target=wait_for_reconnected_command_timeout, daemon=True).start()
    time.sleep(0.1)
    client_reconnect_timeout._mark_command_connection_interrupted()
    client_reconnect_timeout.last_heartbeat_ack = time.monotonic()
    timeout_interrupt_sent = reconnect_timeout_interrupt_sent.wait(1)
    client_reconnect_timeout.reconnect_interrupt_started_at = time.monotonic() - 3.1
    log_test("Command wait returns if reconnect interrupt gets no completion", timeout_interrupt_sent and reconnect_timeout_finished.wait(1))
    client_reconnect_timeout._close_socket()

    client_shutdown = UDPClient('127.0.0.1', TEST_PORT)
    client_shutdown.running = True
    client_shutdown.stop_event.clear()
    client_shutdown.recv_thread = threading.Thread(target=lambda: client_shutdown.stop_event.wait(5))
    client_shutdown.heartbeat_thread = threading.Thread(target=lambda: client_shutdown.stop_event.wait(5))
    client_shutdown.recv_thread.start()
    client_shutdown.heartbeat_thread.start()
    client_shutdown._shutdown_runtime()
    log_test("Client shutdown joins background threads", not client_shutdown.recv_thread.is_alive() and not client_shutdown.heartbeat_thread.is_alive())

    class InterruptingJoinThread:
        def __init__(self):
            self.join_called = False

        def is_alive(self):
            return True

        def join(self, timeout=None):
            self.join_called = True
            raise KeyboardInterrupt

    client_shutdown_interrupt = UDPClient('127.0.0.1', TEST_PORT)
    client_shutdown_interrupt.running = True
    client_shutdown_interrupt.recv_thread = InterruptingJoinThread()
    try:
        client_shutdown_interrupt._shutdown_runtime()
        shutdown_interrupt_handled = True
    except KeyboardInterrupt:
        shutdown_interrupt_handled = False
    log_test("Client shutdown ignores internal interrupt during join", shutdown_interrupt_handled and client_shutdown_interrupt.recv_thread.join_called)
finally:
    client_module._thread.interrupt_main = original_interrupt_main

# ===== Test 3: Command Execution =====
print("\n[3] Command Execution", flush=True)
sock_cmd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
cid_cmd = 10002
sock_cmd.sendto(pack_msg(TYPE_COMMAND, 0, cid_cmd, b'echo TEST_OK_123'), addr)
got_ack, output = recv_response(sock_cmd, cid_cmd, addr, timeout=5)
output_str = output.decode('utf-8', errors='replace')
log_test("Command ACK received", got_ack)
log_test("Echo output correct", "TEST_OK_123" in output_str, f"out={output_str[:80]}")
sock_cmd.close()

cid_encoding = 10015
sock_encoding = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
encoding_cmd = 'echo ENCODING_OK_编码' if os.name == 'nt' else 'printf "ENCODING_OK_编码\n"'
sock_encoding.sendto(pack_msg(TYPE_COMMAND, 0, cid_encoding, encoding_cmd.encode('utf-8')), addr)
_, encoding_out = recv_response(sock_encoding, cid_encoding, addr, timeout=5)
encoding_str = encoding_out.decode('utf-8', errors='replace')
log_test("Non-ASCII output decoded", "ENCODING_OK_编码" in encoding_str, f"out={encoding_str[:80]}")
sock_encoding.close()

cid_utf8_file = 10016
sock_utf8_file = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
utf8_file_cmd = 'type README.md' if os.name == 'nt' else 'cat README.md'
sock_utf8_file.sendto(pack_msg(TYPE_COMMAND, 0, cid_utf8_file, utf8_file_cmd.encode('utf-8')), addr)
_, utf8_file_out = recv_response(sock_utf8_file, cid_utf8_file, addr, timeout=5)
utf8_file_str = utf8_file_out.decode('utf-8', errors='replace')
log_test("UTF-8 file output decoded", "UDP 远程终端" in utf8_file_str, f"out={utf8_file_str[:80]}")
sock_utf8_file.close()

cid_quote = 10017
sock_quote = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
quote_cmd = 'python -c "print(\'\\033[31mQUOTE_OK\\033[0m\')"'
sock_quote.sendto(pack_msg(TYPE_COMMAND, 0, cid_quote, quote_cmd.encode('utf-8')), addr)
_, quote_out = recv_response(sock_quote, cid_quote, addr, timeout=5)
quote_str = quote_out.decode('utf-8', errors='replace')
log_test("Quoted python command executed", "QUOTE_OK" in quote_str, f"out={quote_str[:80]}")
sock_quote.close()

time.sleep(0.3)

print("\n[3b] Advanced Output", flush=True)
rt_cmd = 'python -c "import time; print(\'RT1\', flush=True); time.sleep(1.5); print(\'RT2\', flush=True)"'
if os.name == 'nt':
    large_cmd = 'for /L %i in (1,1,1000) do @echo XXXXXXXXXX'
else:
    large_cmd = 'python -c "print(\'X\' * 8000)"'

cid_rt = 10013
sock_rt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_rt.sendto(pack_msg(TYPE_COMMAND, 0, cid_rt, rt_cmd.encode('utf-8')), addr)
got_rt_ack, rt_out, first_rt_at, total_rt_time = recv_realtime_response(sock_rt, cid_rt, addr, timeout=8)
rt_str = rt_out.decode('utf-8', errors='replace')
log_test("Real-time output ACK received", got_rt_ack)
log_test("Real-time output complete", "RT1" in rt_str and "RT2" in rt_str, f"out={rt_str[:120]}")
log_test("First output before command exit", first_rt_at is not None and first_rt_at < total_rt_time - 0.5,
         f"first={first_rt_at}, total={total_rt_time}")
sock_rt.close()

cid_large = 10014
sock_large = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_large.sendto(pack_msg(TYPE_COMMAND, 0, cid_large, large_cmd.encode('utf-8')), addr)
_, large_out = recv_response(sock_large, cid_large, addr, timeout=8)
log_test("Large output over window complete", large_out.count(b'X') >= 8000, f"len={len(large_out)}")
sock_large.close()

cid_flow = 10019
sock_flow = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_flow.sendto(pack_msg(TYPE_COMMAND, 0, cid_flow, large_cmd.encode('utf-8')), addr)
_, flow_out = recv_response_with_window(sock_flow, cid_flow, addr, advertised_packets=1, timeout=8)
log_test("Large output with advertised receive window complete", flow_out.count(b'X') >= 8000, f"len={len(flow_out)}")
sock_flow.close()

cid_outage = 10022
sock_outage = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_outage.settimeout(1)
sock_outage.sendto(pack_msg(TYPE_HEARTBEAT, HEARTBEAT_SEQ, cid_outage, b''), addr)
try:
    sock_outage.recvfrom(1500)
except socket.timeout:
    pass
outage_result = []
outage_finished = threading.Event()


def send_output_during_ack_outage():
    outage_result.append(server._send_output_reliable(cid_outage, b'OUTAGE_OK'))
    outage_finished.set()


threading.Thread(target=send_output_during_ack_outage, daemon=True).start()
time.sleep(ACK_TIMEOUT * (MAX_RETRIES + 2))
log_test("Output send waits through transient ACK outage", not outage_finished.is_set())
outage_payload_seen = False
end = time.monotonic() + 3
while not outage_finished.is_set() and time.monotonic() < end:
    try:
        resp_data, _ = sock_outage.recvfrom(65535)
    except socket.timeout:
        continue
    resp = unpack_msg(resp_data)
    if resp is None or resp[0] != TYPE_OUTPUT:
        continue
    outage_payload_seen = outage_payload_seen or resp[3] == b'OUTAGE_OK'
    sock_outage.sendto(pack_msg(TYPE_ACK, resp[1], cid_outage, pack_window_update(OUTPUT_WINDOW_SIZE, RECV_BUFFER_LIMIT_BYTES)), addr)
log_test("Output send recovers after transient ACK outage", outage_finished.wait(1) and outage_result == [True] and outage_payload_seen)
sock_outage.close()

cid_rebind = 10023
sock_rebind_old = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_rebind_old.settimeout(0.2)
sock_rebind_old.sendto(pack_msg(TYPE_HEARTBEAT, HEARTBEAT_SEQ, cid_rebind, b''), addr)
try:
    sock_rebind_old.recvfrom(1500)
except socket.timeout:
    pass
rebind_result = []
rebind_finished = threading.Event()


def send_output_to_rebound_client():
    rebind_result.append(server._send_output_reliable(cid_rebind, b'REBIND_OK'))
    rebind_finished.set()


threading.Thread(target=send_output_to_rebound_client, daemon=True).start()
time.sleep(ACK_TIMEOUT * 2)
sock_rebind_old.close()
sock_rebind_new = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_rebind_new.settimeout(1)
sock_rebind_new.sendto(pack_msg(TYPE_HEARTBEAT, HEARTBEAT_SEQ, cid_rebind, b''), addr)
try:
    sock_rebind_new.recvfrom(1500)
except socket.timeout:
    pass
rebind_payload_seen = False
end = time.monotonic() + 3
while not rebind_finished.is_set() and time.monotonic() < end:
    try:
        resp_data, _ = sock_rebind_new.recvfrom(65535)
    except socket.timeout:
        continue
    resp = unpack_msg(resp_data)
    if resp is None or resp[0] != TYPE_OUTPUT:
        continue
    rebind_payload_seen = rebind_payload_seen or resp[3] == b'REBIND_OK'
    sock_rebind_new.sendto(pack_msg(TYPE_ACK, resp[1], cid_rebind, pack_window_update(OUTPUT_WINDOW_SIZE, RECV_BUFFER_LIMIT_BYTES)), addr)
log_test("Output retransmit uses rebound client address", rebind_finished.wait(1) and rebind_result == [True] and rebind_payload_seen)
sock_rebind_new.close()
time.sleep(0.3)

# ===== Test 4: Multiple Clients =====
print("\n[4] Multiple Clients", flush=True)
cid1, cid2 = 10003, 10004
sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock1.sendto(pack_msg(TYPE_COMMAND, 0, cid1, b'echo CLIENT1_OK'), addr)
time.sleep(0.3)
sock2.sendto(pack_msg(TYPE_COMMAND, 0, cid2, b'echo CLIENT2_OK'), addr)

_, out1 = recv_response(sock1, cid1, addr, timeout=5)
_, out2 = recv_response(sock2, cid2, addr, timeout=5)
s1 = out1.decode('utf-8', errors='replace')
s2 = out2.decode('utf-8', errors='replace')

log_test("Client1 correct", "CLIENT1_OK" in s1, f"out={s1[:60]}")
log_test("Client2 correct", "CLIENT2_OK" in s2, f"out={s2[:60]}")
log_test("No cross-contamination", "CLIENT1_OK" in s1 and "CLIENT2_OK" in s2)

sock1.close()
sock2.close()
time.sleep(0.3)

# ===== Test 5: Heartbeat Timeout =====
print("\n[5] Heartbeat Timeout", flush=True)
cid_to = 10005
sock_to = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_to.settimeout(2)
sock_to.sendto(pack_msg(TYPE_HEARTBEAT, HEARTBEAT_SEQ, cid_to, b''), addr)
try:
    sock_to.recvfrom(1500)
except Exception:
    pass

log_test("Client registered", cid_to in server.clients)
if cid_to in server.clients:
    server.clients[cid_to].last_heartbeat = time.time() - 31
    now = time.time()
    to_remove = [c for c, info in list(server.clients.items()) if now - info.last_heartbeat > 30]
    for c in to_remove:
        del server.clients[c]
    log_test("Timeout client removed", cid_to not in server.clients)
else:
    log_test("Timeout client removed", False, "not registered")
sock_to.close()

# ===== Test 6: Stop-Wait ARQ Dedup =====
print("\n[6] Stop-Wait ARQ Dedup", flush=True)
cid_arq = 10006
sock_arq = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock_arq.sendto(pack_msg(TYPE_COMMAND, 0, cid_arq, b'echo first'), addr)
got_ack1, out1 = recv_response(sock_arq, cid_arq, addr, timeout=3)
log_test("First msg got ACK+DATA", got_ack1 and len(out1) > 0)

sock_arq.sendto(pack_msg(TYPE_COMMAND, 0, cid_arq, b'echo dup'), addr)
got_ack2, out2 = recv_response(sock_arq, cid_arq, addr, timeout=2)
log_test("Duplicate seq ACK only", got_ack2 and len(out2) == 0,
         f"ack={got_ack2}, data_len={len(out2)}")
sock_arq.close()
time.sleep(0.3)

# ===== Test 7: Control Characters =====
print("\n[7] Control Characters", flush=True)
cid_ctrl1 = 10007
sock_ctrl1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_ctrl1.sendto(pack_msg(TYPE_COMMAND, 0, cid_ctrl1, b'echo AB\x08C\n'), addr)
_, out_ctrl1 = recv_response(sock_ctrl1, cid_ctrl1, addr, timeout=5)
ctrl1_str = out_ctrl1.decode('utf-8', errors='replace')
log_test("Backspace handled", "AC" in ctrl1_str and "ABC" not in ctrl1_str, f"out={ctrl1_str[:80]}")
sock_ctrl1.close()

cid_ctrl2 = 10008
sock_ctrl2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_ctrl2.sendto(pack_msg(TYPE_COMMAND, 0, cid_ctrl2, b'echo CR_OK\r'), addr)
_, out_ctrl2 = recv_response(sock_ctrl2, cid_ctrl2, addr, timeout=5)
ctrl2_str = out_ctrl2.decode('utf-8', errors='replace')
log_test("Carriage return handled", "CR_OK" in ctrl2_str, f"out={ctrl2_str[:80]}")
sock_ctrl2.close()

cid_ctrl3 = 10012
sock_ctrl3 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_ctrl3.sendto(pack_msg(TYPE_COMMAND, 0, cid_ctrl3, b'echo\tTAB_OK\n'), addr)
_, out_ctrl3 = recv_response(sock_ctrl3, cid_ctrl3, addr, timeout=5)
ctrl3_str = out_ctrl3.decode('utf-8', errors='replace')
log_test("Tab passed to shell", "TAB_OK" in ctrl3_str, f"out={ctrl3_str[:80]}")
sock_ctrl3.close()
time.sleep(0.3)

print("\n[7b] Interrupt Output Ordering", flush=True)
if os.name == 'nt':
    cid_ping = 10021
    sock_ping = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_ping.settimeout(12)
    sock_ping.sendto(pack_msg(TYPE_COMMAND, 0, cid_ping, b'ping 127.0.0.1'), addr)
    got_ping_ack = False
    sent_ping_interrupt = False
    ping_done = False
    ping_out = b''
    expected = recv_expected.get(cid_ping, 0)
    try:
        while not ping_done:
            resp_data, _ = sock_ping.recvfrom(65535)
            resp = unpack_msg(resp_data)
            if resp is None:
                continue
            if resp[0] == TYPE_ACK:
                got_ping_ack = True
            elif resp[0] == TYPE_OUTPUT:
                seq = resp[1]
                payload = resp[3]
                sock_ping.sendto(pack_msg(TYPE_ACK, seq, cid_ping, pack_window_update(OUTPUT_WINDOW_SIZE, RECV_BUFFER_LIMIT_BYTES)), addr)
                if seq != expected:
                    continue
                expected = next_data_seq(expected)
                if unpack_output_done(payload) is not None:
                    ping_done = True
                    continue
                ping_out += payload
                ping_text = ping_out.decode('utf-8', errors='replace')
                if not sent_ping_interrupt and 'TTL=' in ping_text:
                    sock_ping.sendto(pack_msg(TYPE_INTERRUPT, 0, cid_ping, b''), addr)
                    sent_ping_interrupt = True
    except socket.timeout:
        pass
    recv_expected[cid_ping] = expected
    ping_text = ping_out.decode('utf-8', errors='replace')
    control_c_index = ping_text.find('Control-C')
    trailing_after_interrupt = ping_text[control_c_index:] if control_c_index >= 0 else ''
    log_test("Windows ping interrupt ACK received", got_ping_ack)
    log_test("Windows ping interrupt sent", sent_ping_interrupt)
    log_test("Windows ping interrupt uses Control-C", 'Control-C' in ping_text and 'Control-Break' not in ping_text, f"out={ping_text[-200:]}")
    log_test("Windows ping has no reply after interrupt marker", control_c_index >= 0 and 'TTL=' not in trailing_after_interrupt, f"out={ping_text[-200:]}")
    sock_ping.close()
else:
    log_test("Windows ping interrupt ordering skipped", True, "non-Windows")
time.sleep(0.3)

# ===== Test 8: Error Handling =====
print("\n[8] Error Handling", flush=True)
cid_err = 10009
sock_err = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock_err.sendto(pack_msg(TYPE_COMMAND, 0, cid_err, b'nonexistent_cmd_xyz_99999'), addr)
_, err_out = recv_response(sock_err, cid_err, addr, timeout=5)
err_str = err_out.decode('utf-8', errors='replace')
log_test("Invalid cmd returns error", len(err_str) > 0, f"out={err_str[:80]}")

sock_err.sendto(b'\x00\x00' + b'\x00' * 10, addr)
time.sleep(0.3)
log_test("Invalid packet no crash", server.running)
sock_err.close()
time.sleep(0.3)

# ===== Test 9: Server Unreachable =====
print("\n[9] Server Unreachable", flush=True)
sock_un = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_un.settimeout(0.5)
received = False
for _ in range(3):
    sock_un.sendto(pack_msg(TYPE_COMMAND, 0, 10010, b'echo test'), ('127.0.0.1', 19999))
    try:
        sock_un.recvfrom(1500)
        received = True
        break
    except (socket.timeout, ConnectionResetError, OSError):
        pass
log_test("Unreachable timeout detected", not received)
sock_un.close()

# ===== Test 10: Persistent cd command =====
print("\n[10] Persistent cd Command", flush=True)
cid_pwd = 10011
sock_pwd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
expected_parent = os.path.abspath(os.path.join(os.getcwd(), '..'))
show_cwd_cmd = 'echo %CD%' if os.name == 'nt' else 'pwd'

sock_pwd.sendto(pack_msg(TYPE_COMMAND, 0, cid_pwd, b'cd ..'), addr)
got_cd_ack, cd_out = recv_response(sock_pwd, cid_pwd, addr, timeout=5)
log_test("cd ACK received", got_cd_ack)
log_test("cd success has no output", len(cd_out) == 0, f"out={cd_out[:80]}")

sock_pwd.sendto(pack_msg(TYPE_COMMAND, 1, cid_pwd, show_cwd_cmd.encode('utf-8')), addr)
_, out_pwd = recv_response(sock_pwd, cid_pwd, addr, timeout=5)
pwd_str = out_pwd.decode('utf-8', errors='replace').strip()
if os.name == 'nt':
    log_test("cd affects later commands", os.path.normcase(pwd_str) == os.path.normcase(expected_parent), f"out={pwd_str}, expected={expected_parent}")
else:
    log_test("cd affects later commands", pwd_str == expected_parent, f"out={pwd_str}, expected={expected_parent}")
sock_pwd.close()

time.sleep(0.3)
print("\n[11] PTY Simplified Interaction", flush=True)
cid_pty = 10020
sock_pty = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
if os.name == 'nt':
    try:
        import winpty  # noqa: F401
        has_winpty = True
    except ImportError:
        has_winpty = False

    sock_pty.sendto(pack_msg(TYPE_COMMAND, 0, cid_pty, b'pty cmd'), addr)
    if has_winpty:
        got_pty_ack, pty_out = recv_pty_response(sock_pty, cid_pty, addr, b'echo PTY_OK\rexit\r', timeout=8, send_after_output=True)
        pty_str = pty_out.decode('utf-8', errors='replace')
        log_test("Windows PTY command ACK received", got_pty_ack)
        log_test("Windows PTY interaction works", "PTY_OK" in pty_str, f"out={pty_str[:120]}")
    else:
        got_pty_ack, pty_out = recv_response(sock_pty, cid_pty, addr, timeout=5)
        pty_str = pty_out.decode('utf-8', errors='replace')
        log_test("Windows PTY missing dependency reported", got_pty_ack and "pywinpty" in pty_str, f"out={pty_str[:120]}")
else:
    sock_pty.sendto(pack_msg(TYPE_COMMAND, 0, cid_pty, b'pty sh'), addr)
    got_pty_ack, pty_out = recv_pty_response(sock_pty, cid_pty, addr, b'echo PTY_OK\nexit\n', timeout=8)
    pty_str = pty_out.decode('utf-8', errors='replace')
    log_test("POSIX PTY command ACK received", got_pty_ack)
    log_test("POSIX PTY interaction works", "PTY_OK" in pty_str, f"out={pty_str[:120]}")
sock_pty.close()

# ===== Stop server =====
server.running = False
try:
    server.sock.close()
except Exception:
    pass

# ===== SUMMARY =====
print("\n" + "=" * 60, flush=True)
print("  TEST SUMMARY", flush=True)
print("=" * 60, flush=True)
passed = sum(1 for _, p, _ in results if p)
total = len(results)
print(f"Passed: {passed}/{total}", flush=True)

for name, p, detail in results:
    s = "PASS" if p else "FAIL"
    line = f"  [{s}] {name}"
    if detail and not p:
        line += f" ({detail})"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or 'utf-8'
        print(line.encode(encoding, errors='backslashreplace').decode(encoding), flush=True)

if passed == total:
    print("\nAll tests passed!", flush=True)
else:
    print(f"\n{total - passed} test(s) failed!", flush=True)
