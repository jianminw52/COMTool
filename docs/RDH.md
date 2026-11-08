# Remote Debug Host（rdh 0.1.0）

现场 Windows 程序管理一只串口：人工选择端口和波特率、显式连接、看日志、按精确字节批准远程写。工程师和 bot 使用同一套新 CLI。这是新契约 `rdh/v1`，不兼容旧 Box 主题、`boot` / `seq` / `at_ms`、旧 JSON 或旧退出码。

旧实验保留在 `cursor/remote-debug-host-50d2` 与 PR #1，本分支不修改那条历史。

## 分层

| 路径 | 职责 |
| --- | --- |
| `rdh/domain/` | 身份、会话、摘要、有效期、幂等、执行门闩、CLI 退出码。只有标准库 |
| `rdh/app/` | 唯一状态泵和用例。不导入 Qt、pyserial、socket、适配器 |
| `rdh/ui/` | 左设备、中日志、按需协助。只提交用例 |
| `rdh/remote/`、`rdh/cli/` | `rdh/v1` 编解码、MQTT、CLI。不能批准写 |
| `rdh/adapters/` | 串口、审计文件、日志文件、凭据、设置 |
| `rdh/bootstrap.py` | 组装。不写审批策略 |

一个进程里只有一个串口所有者。GUI 和远端走同一个 `Control.submit`。暂停滚动只改视图。

## 契约

主题（QoS 0，不 retain，不离线排队）：

- `rdh/v1/hosts/<host>/requests/<principal>`
- `rdh/v1/hosts/<host>/responses/<principal>`
- `rdh/v1/hosts/<host>/events/<principal>`

账号 ACL 只能发自己的请求、订自己的结果。Host 用主题映射 Principal。payload 里的 `principal` 或 MQTT client id 都不能当身份。broker 不能批准。

`uart.write` 的摘要绑定动作、目标会话、规范化有效期和精确字节。同一调用方的 `operation_id` 加同一摘要返回已知结果；内容变了是 `conflict`。表满拒绝新键，不删已执行记录。

UART 完成时 `success_scope=transport`，`device_outcome=unknown`。没有设备退出码。

| 阶段 | CLI 退出码 |
| --- | --- |
| `denied` / `expired` | 2 |
| `rejected` / `conflict` / `unsupported` | 4 |
| 完整写出，且任务只要求传输 | 0 |
| `partially_written` / `device_failed` | 5 |
| `unknown` / `delivery_unknown`，或等待结束仍未完成 | 3 |
| 队列满 / 幂等表满（`busy`） | 6 |
| 参数或本地传输失败 | 64 / 6 |
| 本地中断 | 130，不代表远端已取消 |

## 启动 Host

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-rdh.txt
python -m rdh --simulate
```

`--simulate` 使用脚本设备，窗口横幅写明「模拟 · 非现场实机」。不带该参数时用本机串口适配器，横幅是「本地串口 · 未做现场验收」。端口和波特率没有默认值，刷新只枚举，连接才打开。DTR/RTS 默认关，驱动不保证开口瞬间绝不复位。

设置文件是 `rdh-settings.json`，只放非秘密字段。程序拒绝读取或改写 `host.json`。密码放在凭据端口，不进 JSON、审计或诊断包。

远程默认关闭。`remote_mode=loopback_isolated` 只连接 `127.0.0.1` 或 `localhost`。`remote_mode=tls` 在 CA、证书、私钥和凭据名都齐备之前拒绝启动远程，现场界面仍可本地使用。

## 启动 CLI

工程师和 bot 都用 `python -m rdh.cli`。密码来自环境变量。

```bash
export RDH_PASSWORD='临时口令'
python -m rdh.cli --broker 127.0.0.1 --port 1883 --host-id host \
  --username eng --password-env RDH_PASSWORD observe
python -m rdh.cli ... logs follow --for-seconds 10
python -m rdh.cli ... operation uart.write --hex 414243 \
  --operation-id op-1 --request-id req-1 \
  --target-session t-... --expires-at 2026-11-08T05:00:00Z
python -m rdh.cli ... operation query --operation-id op-1
```

观察成功的退出码 0 只说明调用返回了，不说明目标在线。写出成功的 0 只说明驱动收下字节。

## 依赖与 Windows 打包路线

运行依赖见 `requirements-rdh.txt`（PyQt5、pyserial）。测试另需 pytest。

本环境没有生成 Windows EXE，也没有在 Windows 上跑过测试。后续在现场 Windows x64 上：

```bat
py -3.12 -m pip install -r requirements-rdh.txt pyinstaller
py -3.12 -m PyInstaller --windowed --name rdh-host rdh\__main__.py
```

Linux 上的 PyInstaller 不能当作 Windows 可执行文件。仓库里原有的 `release_windows.yml` 只在发布 COMTool 时打包，这次没有触发发布。`.github/workflows/rdh-tests.yml` 会在拉取请求上跑 Ubuntu 和 Windows 的 pytest；那次 Actions 的结果要以运行日志为准，不能用这次 Linux 通过代替。

## 测试

```bash
QT_QPA_PLATFORM=offscreen python -m pytest \
  tests/test_rdh_domain.py tests/test_rdh_control.py \
  tests/test_rdh_adapters.py tests/test_rdh_boundary.py \
  tests/test_rdh_window.py tests/test_rdh_cli_e2e.py -q
```

覆盖：有效期与幂等容量、审批竞态、部分写、审计失败、慢消费者、PTY 一次写出、Qt 离屏窗口、本地 broker 上的 CLI 审批、ACL、retain、断线不重发，以及本机临时证书的 TLS。

## 实现参数（设计里留作参数的取值）

- 时钟偏差上限 30 秒；审批最长等待 120 秒；略超期只给 50 毫秒，不延长寿命
- 幂等表 256，不逐出
- 控制队列 64，完成队列 32，接收队列 256 条 / 256 KiB
- 单次写最大 64 KiB
- 审计只 `flush`，不是 `fsync`，不称为掉电安全
- 查看授权保持到收回
- 关闭等待约 3 秒；超时记未完成，不重放

## 与已批准设计的差异

- `busy`（队列或幂等表满）使用退出码 6。批准表没有单独的 busy 码
- `rejected` 与冲突、不支持同为 4；`denied` 与过期为 2
- 系统时钟允许有界有效期，快照标明 `unverified_local`，没有 NTP 证明
- 连接期间不能拨动 DTR/RTS，避免一条绕过用例的写
- 原型里的「写入一次」做成当前这一条请求的「批准这一次」
- I/O 和状态泵线程以 daemon 启动，这样 join 超时后进程还能带着失败码退出。这不是干净关闭
- MQTT 只用 QoS 0

## 没有实现，也没有冒充

ADB、网络调试、刷写只在界面标成未实现，请求返回 `unsupported`。诊断包可以导出 Host 状态、选定日志和审计，拒绝秘密字段。没有真实 Windows、真实 COM、RK 板、公网 TLS 或现场人员操作的结果。连接测试或提交代码都不算现场执行成功。
