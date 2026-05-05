# 完成进阶功能计划（不含拔高）

## Context

本项目当前是 Python 版 UDP 远程终端，基础功能已实现：自定义协议、停等 ARQ、命令执行、多客户端、心跳、持久 `cd`、Ctrl+C 远程中断和基础控制字符处理。实验指导书的 3.2 进阶功能要求补齐终端控制字符增强、回退 N 步 ARQ/滑动窗口发送、实时命令输出；本次明确不做 3.3 拔高功能，因此不引入 PTY、终端窗口大小同步、接收端缓冲流量控制，也不承诺 `top`/`vim` 全屏交互。

目标是在保持现有协议头和基础测试兼容的前提下，补齐进阶功能并更新 README 说明。

## Recommended approach

### 1. 扩展通用协议辅助函数

修改 `common.py`：

- 复用现有 `pack_msg`、`unpack_msg`、`next_data_seq`、`MAX_DATA_SIZE`、`pack_output_done`、`unpack_output_done`。
- 增加常量：`OUTPUT_WINDOW_SIZE`、`ACK_TIMEOUT`、`MAX_RETRIES`，用于输出方向滑动窗口发送。
- 增加 `strip_ansi_sequences(data: bytes) -> bytes`，使用 bytes 级正则过滤 ANSI 转义序列，避免因输出编码异常导致过滤失败。
- 增强 `normalize_command_input(text: str)`：保留当前 `\r\n`、`\r`、退格、Delete 处理；增加 ANSI 输入过滤；将 Tab 转成一个空格，满足命令行输入场景。
- 如实现 Go-Back-N 累计 ACK 需要，增加 `prev_data_seq(seq: int) -> int` 或小范围序列比较辅助函数，避免误用心跳保留序号。

### 2. 实现输出方向滑动窗口 / 回退 N 步 ARQ

修改 `server.py` 和 `client.py`，范围仅限服务端到客户端的 `TYPE_OUTPUT`。客户端到服务端命令发送继续使用现有停等 ARQ，降低改动风险并保留命令去重语义。

服务端修改点：

- 在 `ClientInfo` 增加 `send_lock`，保护同一客户端的 `send_seq` 和输出发送状态，避免 stdout/stderr 并发读取时序列号冲突。
- 保留 `_handle_ack` 和 `ack_events[(client_id, seq)]` 机制；发送端把 ACK 当作对应输出分片的确认。
- 拆分当前 `_send_reliable(client_id, data)`：
  - 新增 `_send_output_chunks_reliable(client_id, chunks)`，一次接收多个分片，按窗口批量发送、超时后从窗口基序号开始重传未确认分片，失败时返回 `False`。
  - 新增 `_send_output_reliable(client_id, data)`，只发送普通输出分片，不附加结束标记。
  - 新增 `_send_output_done(client_id)`，将 `pack_output_done(client.cwd)` 作为最后一个可靠输出分片发送。
  - 保留 `_send_reliable(client_id, data)` 作为兼容包装：发送普通数据后再发送 done 标记，供 `cd` 和现有测试路径继续使用。
- 大块输出应按 `MAX_DATA_SIZE` 切片后进入窗口发送；窗口大小使用 `OUTPUT_WINDOW_SIZE`，超时与最大重传使用 `ACK_TIMEOUT`、`MAX_RETRIES`。

客户端修改点：

- 在 `UDPClient.__init__` 增加接收缓冲，例如 `recv_buffer = {}`。
- 抽出 `_process_output_payload(payload)`：先判断 `unpack_output_done(payload)`，若是 done 标记则更新提示符并设置 `output_done_event`；否则对普通输出执行 `strip_ansi_sequences` 后写入 stdout。
- 在 `_recv_loop` 的 `TYPE_OUTPUT` 分支中：
  - 每个合法输出包都回 ACK，维持重传效率。
  - 只有 `seq == recv_expected_seq` 时才处理并推进序列号。
  - 对提前到达的输出包暂存到 `recv_buffer`，等缺失分片到达后按序 drain。
  - 对旧包/重复包只 ACK，不重复显示。
- done 标记也作为普通输出序列的一部分按序处理，确保提示符只在前面输出全部显示后恢复。

说明：指导书写的是“回退 N 步 ARQ / 滑动窗口发送”。当前协议 ACK 已按序列号确认，采用小窗口、有序显示和超时批量重传即可满足滑动窗口发送目标；不实现接收端缓冲自适应流控，因为那属于拔高功能。

### 3. 实现实时命令输出

修改 `server.py` 的 `_execute_and_respond`：

- 用当前 `subprocess.Popen` 结构，不改成 PTY；Windows 继续用 `cmd.exe /c` 与 `CREATE_NEW_PROCESS_GROUP`，POSIX 继续用 `bash -c` 与 `os.setsid`。
- 将 `proc.communicate(timeout=30)` 改为 stdout/stderr 流式读取：
  - `stdout=subprocess.PIPE`、`stderr=subprocess.PIPE` 保持二进制读取。
  - 分别启动 stdout/stderr reader 线程，将读到的 bytes 分片通过 `_send_output_reliable` 立即发送。
  - 主线程等待进程结束并保留 30 秒超时；超时后复用 `_kill_process_tree`，发送 `Error: Command execution timed out\n`，最后发送 done 标记。
  - `finally` 中保持现有 `client.current_process = None` 清理逻辑。
- 保留 `_handle_interrupt`、`_interrupt_process`、`_kill_process_tree`，确保执行中 Ctrl+C 仍能中断当前远程进程。
- 明确限制：未引入 PTY 时，部分程序自身可能因非 TTY 管道而缓冲输出；本次实现“已 flush 的 stdout/stderr 实时传输”，不承诺全屏交互或 PTY 行为。

### 4. 补齐进阶测试

修改 `test_all.py`，保留现有测试并追加：

- `strip_ansi_sequences` 单元测试：`[31mRED[0m` 应变成 `RED`。
- `normalize_command_input` 测试：Tab 转空格、ANSI 输入过滤、原有退格/回车行为继续通过。
- Tab 命令执行测试：发送 `echo\tTAB_OK\n`，输出应包含 `TAB_OK`。
- 实时输出测试：发送 `python -c "import time; print('RT1', flush=True); time.sleep(1); print('RT2', flush=True)"`，读取第一个输出分片并断言 `RT1` 在命令结束前到达，再继续 ACK 到 done。
- 大输出滑动窗口测试：发送 `python -c "print('X' * 8000)"` 或更大输出，验证接收到的内容完整且 done 正常到达。
- 如已抽出 `_process_output_payload`，可追加轻量客户端显示过滤测试；否则以 helper 测试覆盖 ANSI 输出过滤核心逻辑。

### 5. 更新 README 说明

修改 `README.md`：

- 将项目描述从“停等 ARQ”更新为“命令方向停等 ARQ，输出方向滑动窗口可靠传输”。
- 在基础/进阶完成情况中标明已完成：实时输出、输出方向滑动窗口/回退 N 步 ARQ、Tab 和 ANSI 过滤。
- 删除或改写“持续运行命令不保证实时回显”的旧限制，改成“不依赖 PTY，部分程序若自身缓冲输出仍可能延迟”。
- 保留“不支持 vim/top/sudo 交互”等拔高/交互限制。
- 更新测试说明，加入新增进阶验证项。

## Critical files

- `common.py`：协议常量、ANSI 过滤、控制字符规范化、序列辅助函数。
- `server.py`：`ClientInfo`、`_send_reliable`、新增输出窗口发送函数、`_execute_and_respond` 实时输出改造。
- `client.py`：`UDPClient.__init__`、`_recv_loop`、新增输出处理/缓冲函数、ANSI 输出过滤。
- `test_all.py`：新增进阶功能自动化测试。
- `README.md`：同步当前实现能力和限制。

## Verification

自动化验证：

```bash
python test_all.py
```

手动端到端验证：

1. 启动服务端：`python server.py 9999`。
2. 另开终端启动客户端：`python client.py 127.0.0.1 9999`。
3. 基础回归：执行 `echo hello`、`cd ..` 后执行 `pwd` 或 Windows 下 `echo %CD%`。
4. 实时输出：执行 `python -c "import time; print('one', flush=True); time.sleep(2); print('two', flush=True)"`，应立即看到 `one`，约 2 秒后看到 `two`。
5. 大输出/窗口传输：执行 `python -c "print('A' * 20000)"`，输出应完整且提示符正常返回。
6. Ctrl+C：执行持续输出命令后按 Ctrl+C，应中断服务端进程并返回提示符。
7. Tab：输入或粘贴 `echo<Tab>TAB_OK`，应等价于 `echo TAB_OK`。
8. ANSI：执行 `python -c "print('\033[31mRED_TEXT\033[0m')"`，客户端应显示 `RED_TEXT`，不显示原始转义符。

## Out of scope

- PTY 伪终端。
- `top` / `vim` / 交互式全屏程序支持。
- 终端窗口大小同步。
- 按接收端缓冲区动态调整窗口的流量控制。
