# 现场调试台

这是 COMTool 仓库里的现场 Host，给一台 Windows x86_64 电脑管理一块板的串口。工程师和 bot 使用同一套 `bot/logbox_cli.py`，经 MQTT 看日志、提交命令。bot 不在现场电脑上自己打开串口。

许可仍是仓库根目录的 LGPL-3.0。Host 代码是在这个 Fork 上新增的，串口参数对齐 `COMTool/conn/conn_serial.py`。盒上的 CLI 来自只读契约 `df5e7df`，不是本仓库上游 Neutree 的代码。

## 运行

```text
python COMTool/host_main.py
```

也可以 `python -m COMTool.host`。需要本仓库已有的 PyQt5 和 pyserial。启动后不会自动选端口、不会猜波特率、不会连接。

1. 左侧选择串口，点「刷新列表」只枚举，不打开。
2. 选择波特率。DTR、RTS 默认关闭。打开端口时，部分驱动仍可能产生瞬态，程序不能保证目标板绝不复位。
3. 点「连接」。端口和波特率收成左侧设备卡，日志在中间。
4. 「暂停滚动」只停画面跟随，记录继续。
5. 「连接设置」填写 device id 和 broker。留空则保持离线。密码只在本机 `host.json`，不写入操作记录。
6. 接入后才能「打开远程协助」。允许查看日志和写串口是两件事。写命令默认关闭。

重连只针对这次亲手选过、并且仍然是同一只口的设备。列表里找不到时，不会改接另一块板。

## 工程师和 bot

先在现场打开查看权限。写串口时，现场窗口会显示这一条的原文和影响，批准后才写入。

```text
python bot/logbox_cli.py --device-id 你的id --op observe
python bot/logbox_cli.py --device-id 你的id --op live --boot-id 观察结果 --seq 下一条控制序号 --at-ms 观察结果的mono_ms --live-log ./box.jsonl
python bot/logbox_cli.py --device-id 你的id --text ping --boot-id 同一个 --seq 下一条串口序号 --at-ms 新的mono_ms
```

主题仍是 `logbox/<id>/cmd` 和 `logbox/<id>/state`。默认批准等待 7 秒，要短于 CLI 的 `--timeout`（默认 8 秒）。超时未批准不会补写。同一序号的重试在还没写入时只保留一条请求；写入或拒绝之后，旧序号不会再执行。

CLI 只要对上 `request_id` 就会返回 0，并把 `phase` 打出来。`phase=denied`、`unsupported`、`approval_timeout` 都不是目标执行成功。`uart=sent` 只表示字节进了串口，`completion=unknown`。

## 还没有做

ADB、网络调试、刷机、百度上传不会执行，界面上标为未实现。RK3588、RK3576、RK3568 以及 Android、嵌入式 Linux 都还没有适配，也没有实机验证。

## Windows 现场还要验

本仓库现有 `pack.py` 用 PyInstaller 把 `COMTool/Main.py` 打成窗口程序，发布流程写的是 Windows 上的 Python 3.9。Host 可以沿同一路线，把入口换成 `COMTool/host_main.py`，并带上 `bot/`。这里没有生成 exe，也没有在 Windows 上跑过。待验：真实 COM 独占、打开瞬间的 DTR/RTS、拔插后的同一设备重连、中文界面、broker TLS。

## 测试

```text
python -m unittest discover -s bot -v
python -m unittest discover -s tests -v
```

`bot/test_cli.py` 里子进程的工作目录改为该文件的上一级，不再写死 `/workspace`。协议断言没有改。`tests/` 里的 PTY 和 127.0.0.1 broker 是模拟，不是 Windows，也不是板卡。
