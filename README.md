# 飞书 Perfecto → QQ 群转发器

这是一个只在本机运行的转发工具。macOS 飞书通知仅用于唤醒；真实消息由飞书用户 API 从固定的 Perfecto 单聊中读取，再通过 QQ 官方机器人 API 转发到已绑定群。

当前支持普通文本和图片。文本按飞书 `message_id` 转发；图片先以飞书用户身份下载，再上传 QQ 取得 `file_info`，最后以 `RICH_MEDIA` 发送。

## 工作方式

1. Swift 程序通过 macOS 辅助功能订阅通知中心的 AX 事件。
2. 发现 Perfecto 通知后，只写一条不含正文和正文派生值的唤醒记录。
3. Python 转发器使用固定 profile `tenant-105183` 定位唯一 Perfecto 联系人和单聊。
4. 根据持久化的 `message_position` 拉取游标之后的消息，并按 `message_id` 去重。
5. 文本直接发送到 QQ；图片临时下载、上传和发送后立即清理。
6. 每条消息成功发送或明确跳过后才推进游标；发送失败不会越过该消息。

通知与消息不是一一对应关系。macOS 可能把连续同文消息折叠成汇总通知，因此通知只负责触发一次 API 同步；一次触发可以取回多条消息。重复通知再次触发时，游标会使结果为空，不会重复转发。

## 前置条件

- macOS 已运行飞书客户端，并允许本程序使用“辅助功能”。
- `lark-cli` 中存在并已授权 profile `tenant-105183`，对应用户为“用户105183”。
- 该账号与 Perfecto 有唯一的点对点会话。
- QQ 机器人密钥已保存在 macOS 钥匙串，目标群已经绑定并允许机器人主动发言。
- Python 3.12、Swift 5.10 或更高版本。

所有飞书 CLI 请求都会显式携带 `--profile tenant-105183`，不会切换或依赖默认 profile。

## 首次检查与绑定

创建 Python 环境并安装 QQ 官方 SDK：

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements-qq.txt
```

检查飞书用户授权、Perfecto 固定会话、QQ 凭证和网关：

```bash
.venv/bin/python qq_bridge.py check \
  --lark-profile tenant-105183 \
  --lark-contact Perfecto
```

如尚未绑定 QQ 群：

```bash
.venv/bin/python qq_bridge.py bind
```

随后在目标群发送 `@qclaw 绑定测试`。群主还需在手机 QQ 的“群设置 → 机器人 → qclaw → 机器人权限设置”中开启“机器人主动在群聊内发言”。

测试 QQ 主动文本消息：

```bash
.venv/bin/python qq_bridge.py test
```

## 启动转发

双击 `start-forwarder.command`。脚本会：

- 构建 Release 版通知监听器；
- 从当前通知文件末尾建立或恢复字节游标；
- 从 Perfecto 会话当前最新位置建立或恢复消息游标；
- 同时启动通知监听器和 Python 转发器。

启动时不会重放历史消息。异常重启后会从已确认的消息位置继续。

按 `Control+C` 停止。脚本会同时结束通知监听器和转发器。

如果明确需要放弃尚未处理的历史记录并把两类游标都移到当前末尾，可手动执行：

```bash
.venv/bin/python qq_bridge.py prime \
  --force-end \
  --lark-profile tenant-105183 \
  --lark-contact Perfecto
```

## 单独检查通知监听

`start.command` 只启动本地通知监听，不访问飞书或 QQ API：

```bash
./start.command --duration 60
```

只检查辅助功能权限和运行环境：

```bash
swift run lark-notification-probe --check
```

新版唤醒记录不包含飞书正文：

```json
{
  "app": "Lark",
  "bundle_id": "com.electron.lark",
  "observed_at": "2026-09-01T10:00:00.000Z",
  "schema_version": 2,
  "source": "macos_accessibility",
  "title": "Perfecto",
  "type": "notification_wakeup"
}
```

## 2026-09-01 本机验收结果

- 普通文本通知可唤醒固定会话 API，并唯一定位真实消息 ID。
- 同一通知重复解析得到相同消息 ID。
- 两条连续同文消息在 API 中位置相邻、消息 ID 不同；游标模型会转发两条。
- macOS 会把第二条同文通知折叠成汇总通知，证明不能用通知正文或通知数量做去重。
- 飞书图片以用户身份下载成功，样本为 JPEG、196,240 字节、736 × 768。
- 同一图片通过 QQ 官方 SDK 上传取得 `file_info`，以 `RICH_MEDIA` 发送并在测试群可见。

## 隐私与限制

- 通知 JSONL 只保存唤醒所需的应用名、标题和时间，不保存消息正文或正文派生值。
- 飞书 API 返回的文本只在内存中处理；图片只存在于系统临时目录，发送结束后自动删除。
- `.qq-forwarder-state.json` 保存 QQ 群绑定、通知字节位置、飞书会话标识、消息位置和最近消息 ID，不保存飞书正文或图片。
- 静音、关闭飞书通知、专注模式、飞书前台抑制通知或辅助功能权限失效时，macOS 可能不产生唤醒事件；没有唤醒事件就不会主动同步。
- 当前只转发 Perfecto 单聊中的普通文本和图片；其他消息类型会记录类型并推进游标，不会误当文本发送。
