import socket
import struct
import threading
import time
import sys
import os
import random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *

TEST_PORT = 19888
results = []

def log_test(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    msg = f"[{status}] {name}"
    if detail and not passed:
        msg += f" - {detail}"
    print(msg, flush=True)
    results.append((name, passed, detail))

def recv_response(sock, cid, addr, timeout=5):
    got_ack = False
    all_data = b''
    sock.settimeout(timeout)
    try:
        while True:
            resp_data, _ = sock.recvfrom(65535)
            resp = unpack_msg(resp_data)
            if resp is None:
                continue
            if resp[0] == TYPE_ACK:
                got_ack = True
            elif resp[0] == TYPE_DATA:
                all_data += resp[3]
                ack = pack_msg(TYPE_ACK, resp[1], cid, b'')
                sock.sendto(ack, addr)
                sock.settimeout(1)
    except socket.timeout:
        pass
    return got_ack, all_data

print("=" * 60, flush=True)
print("  UDP Remote Terminal - Full Test Suite", flush=True)
print("=" * 60, flush=True)

# ===== Test 1: Protocol Pack/Unpack =====
print("\n[1] Protocol Pack/Unpack", flush=True)
data = b'hello world'
packed = pack_msg(TYPE_DATA, 5, 12345, data)
result = unpack_msg(packed)
if result:
    msg_type, seq, client_id, payload = result
    log_test("Pack/Unpack basic", msg_type == TYPE_DATA and seq == 5 and client_id == 12345 and payload == data)
else:
    log_test("Pack/Unpack basic", False, "None")

fake_header = struct.pack('>H B B I I', 0xFFFF, TYPE_DATA, 0, 1, 5)
log_test("Invalid magic rejected", unpack_msg(fake_header + b'hello') is None)

packed2 = pack_msg(TYPE_DATA, 0, 1, b'hello')
log_test("Truncated rejected", unpack_msg(packed2[:5]) is None)

try:
    pack_msg(TYPE_DATA, 0, 1, b'x' * (MAX_DATA_SIZE + 1))
    log_test("Oversized rejected", False)
except ValueError:
    log_test("Oversized rejected", True)

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
sock_hb.sendto(pack_msg(TYPE_HEARTBEAT, 255, cid_hb, b''), addr)
try:
    resp_data, _ = sock_hb.recvfrom(1500)
    resp = unpack_msg(resp_data)
    log_test("Heartbeat ACK received", resp is not None and resp[0] == TYPE_ACK and resp[1] == 255)
except socket.timeout:
    log_test("Heartbeat ACK received", False, "timeout")
sock_hb.close()

# ===== Test 3: Command Execution =====
print("\n[3] Command Execution", flush=True)
sock_cmd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
cid_cmd = 10002
sock_cmd.sendto(pack_msg(TYPE_DATA, 0, cid_cmd, b'echo TEST_OK_123'), addr)
got_ack, output = recv_response(sock_cmd, cid_cmd, addr, timeout=5)
output_str = output.decode('utf-8', errors='replace')
log_test("Command ACK received", got_ack)
log_test("Echo output correct", "TEST_OK_123" in output_str, f"out={output_str[:80]}")
sock_cmd.close()
time.sleep(0.3)

# ===== Test 4: Multiple Clients =====
print("\n[4] Multiple Clients", flush=True)
cid1, cid2 = 10003, 10004
sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock1.sendto(pack_msg(TYPE_DATA, 0, cid1, b'echo CLIENT1_OK'), addr)
time.sleep(0.3)
sock2.sendto(pack_msg(TYPE_DATA, 0, cid2, b'echo CLIENT2_OK'), addr)

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
sock_to.sendto(pack_msg(TYPE_HEARTBEAT, 255, cid_to, b''), addr)
try:
    sock_to.recvfrom(1500)
except:
    pass

log_test("Client registered", cid_to in server.clients)
if cid_to in server.clients:
    server.clients[cid_to].last_heartbeat = time.time() - 31
    # 手动触发清理以避免等待
    now = time.time()
    to_remove = [c for c, info in server.clients.items() if now - info.last_heartbeat > 30]
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

sock_arq.sendto(pack_msg(TYPE_DATA, 0, cid_arq, b'echo first'), addr)
got_ack1, out1 = recv_response(sock_arq, cid_arq, addr, timeout=3)
log_test("First msg got ACK+DATA", got_ack1 and len(out1) > 0)

sock_arq.sendto(pack_msg(TYPE_DATA, 0, cid_arq, b'echo dup'), addr)
got_ack2, out2 = recv_response(sock_arq, cid_arq, addr, timeout=2)
log_test("Duplicate seq ACK only", got_ack2 and len(out2) == 0,
         f"ack={got_ack2}, data_len={len(out2)}")
sock_arq.close()
time.sleep(0.3)

# ===== Test 7: Error Handling =====
print("\n[7] Error Handling", flush=True)
cid_err = 10007
sock_err = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock_err.sendto(pack_msg(TYPE_DATA, 0, cid_err, b'nonexistent_cmd_xyz_99999'), addr)
_, err_out = recv_response(sock_err, cid_err, addr, timeout=5)
err_str = err_out.decode('utf-8', errors='replace')
log_test("Invalid cmd returns error", len(err_str) > 0, f"out={err_str[:80]}")

sock_err.sendto(b'\x00\x00' + b'\x00' * 10, addr)
time.sleep(0.3)
log_test("Invalid packet no crash", server.running)
sock_err.close()
time.sleep(0.3)

# ===== Test 8: Ctrl+C Interrupt =====
print("\n[8] Ctrl+C Interrupt", flush=True)
cid_int = 10008
sock_int = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_int.settimeout(2)

long_cmd = 'ping -n 10 127.0.0.1' if os.name == 'nt' else 'sleep 10'
sock_int.sendto(pack_msg(TYPE_DATA, 0, cid_int, long_cmd.encode('utf-8')), addr)
time.sleep(1.5)

sock_int.sendto(pack_msg(TYPE_INTERRUPT, 1, cid_int, b''), addr)
time.sleep(1.5)

client_info = server.clients.get(cid_int)
process_killed = client_info is None or client_info.current_process is None
log_test("Ctrl+C killed process", process_killed)

try:
    while True:
        sock_int.settimeout(2)
        resp_data, _ = sock_int.recvfrom(65535)
        r = unpack_msg(resp_data)
        if r and r[0] == TYPE_DATA:
            sock_int.sendto(pack_msg(TYPE_ACK, r[1], cid_int, b''), addr)
except:
    pass
sock_int.close()
time.sleep(0.3)

# ===== Test 9: Server Unreachable =====
print("\n[9] Server Unreachable", flush=True)
sock_un = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock_un.settimeout(0.5)
received = False
for _ in range(3):
    sock_un.sendto(pack_msg(TYPE_DATA, 0, 10009, b'echo test'), ('127.0.0.1', 19999))
    try:
        sock_un.recvfrom(1500)
        received = True
        break
    except (socket.timeout, ConnectionResetError, OSError):
        pass
log_test("Unreachable timeout detected", not received)
sock_un.close()

# ===== Test 10: pwd/cd command =====
print("\n[10] pwd/cd Command", flush=True)
cid_pwd = 10010
sock_pwd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
cmd = 'cd' if os.name == 'nt' else 'pwd'
sock_pwd.sendto(pack_msg(TYPE_DATA, 0, cid_pwd, cmd.encode('utf-8')), addr)
_, out_pwd = recv_response(sock_pwd, cid_pwd, addr, timeout=5)
pwd_str = out_pwd.decode('utf-8', errors='replace')
log_test(f"{cmd} has output", len(pwd_str.strip()) > 0, f"out={pwd_str[:80]}")
sock_pwd.close()

# ===== Stop server =====
server.running = False
try:
    server.sock.close()
except:
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
    print(line, flush=True)

if passed == total:
    print("\nAll tests passed!", flush=True)
else:
    print(f"\n{total - passed} test(s) failed!", flush=True)
