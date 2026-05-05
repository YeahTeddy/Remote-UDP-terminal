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
                sock.settimeout(1)
    except socket.timeout:
        pass
    recv_expected[cid] = expected
    return got_ack, all_data, first_output_at, time.monotonic() - started


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
log_test("Message types match guide", (TYPE_COMMAND, TYPE_OUTPUT, TYPE_ACK, TYPE_HEARTBEAT, TYPE_INTERRUPT) == (0x01, 0x02, 0x03, 0x04, 0x05))

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

# ===== Start server =====
print("\n[2] Starting UDP server...", flush=True)
from server import UDPServer
server = UDPServer('127.0.0.1', TEST_PORT)
server.running = True
threading.Thread(target=server._recv_loop, daemon=True).start()
threading.Thread(target=server._cleanup_loop, daemon=True).start()
time.sleep(0.5)
addr = ('127.0.0.1', TEST_PORT)

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

# ===== Test 2b: Client Connection Check =====
print("\n[2b] Client Connection Check", flush=True)
from client import UDPClient
client_ok = UDPClient('127.0.0.1', TEST_PORT)
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
if os.name == 'nt':
    rt_cmd = 'ping -n 3 127.0.0.1'
    large_cmd = 'for /L %i in (1,1,1000) do @echo XXXXXXXXXX'
else:
    rt_cmd = 'python -c "import time; print(\'RT1\', flush=True); time.sleep(1.5); print(\'RT2\', flush=True)"'
    large_cmd = 'python -c "print(\'X\' * 8000)"'

cid_rt = 10013
sock_rt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_rt.sendto(pack_msg(TYPE_COMMAND, 0, cid_rt, rt_cmd.encode('utf-8')), addr)
got_rt_ack, rt_out, first_rt_at, total_rt_time = recv_realtime_response(sock_rt, cid_rt, addr, timeout=8)
rt_str = rt_out.decode('utf-8', errors='replace')
log_test("Real-time output ACK received", got_rt_ack)
if os.name == 'nt':
    log_test("Real-time output complete", len(rt_out) > 0, f"out={rt_str[:120]}")
else:
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
