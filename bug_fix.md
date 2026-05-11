# Windows 客户端连接 Linux 服务端时 vim 大文件卡死问题修复

## 问题现象

当 Windows 客户端连接 Linux 服务端，并在远程终端中执行类似下面的交互式命令时：

```bash
vim README.md
```

如果文件内容较多，尤其包含中文等 UTF-8 多字节字符，客户端可能直接卡死。与此同时，服务端仍能收到客户端心跳，但会持续打印输出包重传日志，例如：

```text
Retry 5 for client 1482139617 seq 3
Retry 5 for client 1482139617 seq 4
Retry 5 for client 1482139617 seq 5
Retry 10 for client 1482139617 seq 3
Retry 10 for client 1482139617 seq 4
Retry 10 for client 1482139617 seq 5
```

这说明客户端连接没有完全断开，但服务端一直收不到部分输出包的 ACK。

## 根本原因

问题出在客户端接收 Linux PTY 输出后的本地显示路径。

服务端从 Linux PTY 读取 vim 输出后，会按 UDP 最大载荷大小把输出切成多个数据包发送。这个切分是按字节进行的，不保证刚好落在 UTF-8 字符边界上。

Linux vim 输出中如果包含中文字符，一个 UTF-8 字符可能由多个字节组成。当 UDP 分片正好把这个字符切开时，Windows 客户端原先会把每个 UDP 包的原始字节直接写入本地标准输出：

```python
out = getattr(sys.stdout, 'buffer', None)
if out is not None:
    out.write(payload)
    out.flush()
```

在 Windows 控制台环境中，这种写入路径可能触发控制台编码转换。被切开的 UTF-8 多字节字符会导致本地输出失败或卡住。

更关键的是，客户端原来的处理顺序是：

1. 收到服务端 TYPE_OUTPUT 包。
2. 先把 payload 写到本地终端。
3. 写入成功后才向服务端发送 ACK。

因此一旦本地显示阶段失败，该输出包的 ACK 就不会发送。服务端的滑动窗口发送逻辑会认为这些 seq 没有确认，于是持续重传同一批输出包，形成卡死现象。

## 修复方法

修复分为两部分。

### 1. Windows 交互式输出使用 UTF-8 增量解码

为客户端增加交互式输出的 UTF-8 增量解码器：

```python
self.interactive_stdout_decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')
```

在 Windows 交互式模式下，不再直接把 UDP payload 原始字节写入 stdout，而是先通过增量解码器解码：

```python
text = self.interactive_stdout_decoder.decode(payload)
if text:
    sys.stdout.write(text)
    sys.stdout.flush()
```

这样即使一个中文字符被拆到两个 UDP 包中，解码器也会保留未完成的半个字符，等下一个包到达后再一起解码输出。

每次进入新的交互式命令时，重置该解码器，避免上一次 PTY 会话残留状态影响下一次命令：

```python
self._reset_interactive_stdout_decoder()
```

### 2. 本地显示异常不再阻止 ACK

客户端在处理输出 payload 时，将本地显示操作包在异常保护中：

```python
try:
    if self.interactive_mode:
        self._write_interactive_output(payload)
    else:
        sys.stdout.write(strip_ansi_sequences(payload).decode('utf-8', errors='replace'))
        sys.stdout.flush()
except Exception:
    pass
```

这样即使本地终端显示失败，客户端仍然可以完成输出包处理并推进接收序号，随后向服务端发送 ACK。协议层不会因为显示层异常而进入无限重传。

## 修改文件

本次修复主要涉及：

- `client.py`
  - 新增 `codecs` 导入。
  - 新增 `interactive_stdout_decoder`。
  - 新增 `_reset_interactive_stdout_decoder()`。
  - 新增 `_write_interactive_output()`。
  - 修改 `_process_output_payload()`，隔离本地显示异常。
  - 修改 `_run_interactive_until_done()`，进入交互式命令时重置解码器。

- `test_all.py`
  - 增加 Windows PTY 输出跨分片 UTF-8 解码测试。
  - 增加本地显示失败时 ACK 不受影响的回归测试。

## 验证结果

已运行完整测试：

```bash
python test_all.py
```

结果：

```text
Passed: 80/80
All tests passed!
```

建议在真实环境中继续验证：Windows 客户端连接 Linux 服务端后执行 `vim README.md`，观察客户端是否还能正常交互，以及服务端是否不再持续打印 `Retry ... seq ...`。