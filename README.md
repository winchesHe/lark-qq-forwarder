# 飞书本地通知验证器

这是一个只在本机运行的飞书通知捕获工具。它不读取飞书私有数据库；单独运行 `start.command` 时不会访问网络，也不会转发消息。

工具通过 macOS 辅助功能观察通知中心，只记录显示名称为 `Lark`、`Feishu` 或 `飞书` 的新增通知。每条记录会同时输出到终端，并追加到 `lark-notifications.jsonl`。

## 开始验证

1. 双击 `start.command`。
2. 首次运行时，macOS 会要求辅助功能权限。打开“系统设置 → 隐私与安全性 → 辅助功能”，允许启动该脚本的终端应用。
3. 权限生效后重新双击 `start.command`。
4. 让其他账号向当前飞书发送几条测试消息。
5. 查看终端输出，或打开同目录下的 `lark-notifications.jsonl`。
6. 在终端按 `Control+C` 停止。

## 转发到 QQ 群

QQ 桥接使用 QQ 官方机器人 WebSocket 和 OpenAPI。AppSecret 保存在 macOS 钥匙串，不会写入项目文件。

首次使用依次运行：

```bash
.venv/bin/python qq_bridge.py check
.venv/bin/python qq_bridge.py bind
.venv/bin/python qq_bridge.py test
```

运行 `bind` 后，在目标 QQ 群中发送 `@qclaw 绑定测试`。群主还需要在手机 QQ 的“群设置 → 机器人 → qclaw → 机器人权限设置”中开启“机器人主动在群聊内发言”；否则被动回复可用，但自动转发会被 QQ 拒绝。

绑定和主动消息测试成功后，双击 `start-forwarder.command`，即可同时启动飞书通知捕获和 QQ 转发。首次建立转发游标时从 JSONL 文件末尾开始，不会重放已有记录；后续异常重启会从已确认的字节位置继续。

## 本机验证结果

2026 年 9 月 1 日在 macOS 15.6 和当前飞书客户端上完成了基础文本通知验证：

- macOS 实际投递 2 条飞书通知，验证器捕获 2 条，当前小样本命中率为 2/2。
- 两次从系统投递到 JSONL 落盘的延迟约为 0.35 秒和 0.33 秒。
- 两条记录的应用标识、标题、正文、原始文本和去重指纹均完整，未出现重复记录。
- 当前结论只覆盖普通文本通知；飞书前台、静音群、图片、文件、卡片和专注模式仍需单独测试。

启动时已有的通知默认不会写入，避免把历史通知误当作新消息。如需检查通知中心里已经存在的飞书通知，可以运行：

```bash
./start.command --include-existing
```

只检查权限和运行环境：

```bash
swift run lark-notification-probe --check
```

限时运行 60 秒：

```bash
./start.command --duration 60
```

## 输出格式

每行是一条独立 JSON，例如：

```json
{
  "app": "Lark",
  "body": "收到请回复",
  "bundle_id": "com.electron.lark",
  "fingerprint": "1c62dc4063dcf03a",
  "observed_at": "2026-09-01T10:00:00.000Z",
  "raw_texts": ["Lark", "王小明", "收到请回复"],
  "schema_version": 1,
  "source": "macos_accessibility",
  "subtitle": "",
  "title": "王小明",
  "type": "notification"
}
```

`raw_texts` 是本阶段最重要的校准字段，可以看出不同类型飞书通知在系统里的真实结构。

## 建议测试矩阵

| 场景 | 期望 |
|---|---|
| 飞书在后台的私聊文本 | 捕获标题和正文 |
| 飞书在后台的群聊文本 | 捕获群名、发送者或正文 |
| 飞书位于前台 | 记录实际是否仍有系统通知 |
| 图片、文件、卡片 | 记录系统提供的摘要文本 |
| 静音群聊 | 确认是否完全没有系统通知 |
| macOS 专注模式 | 确认通知是否延迟或缺失 |

## 隐私与限制

- JSONL 文件可能包含真实消息正文，测试结束后请按需删除。
- 这里只能捕获 macOS 实际展示给用户的通知，不是完整的飞书消息流。
- 静音、关闭预览、飞书前台抑制通知、专注模式和系统通知折叠都可能造成缺失或字段不完整。
- `start.command` 仍然是纯本地模式；只有运行 `qq_bridge.py` 或 `start-forwarder.command` 时才会把新通知正文发送到 QQ 官方接口。
