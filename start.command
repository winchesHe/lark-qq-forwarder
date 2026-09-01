#!/bin/zsh

set -euo pipefail

PROBE_DIR="${0:A:h}"
cd "$PROBE_DIR"

echo "正在构建飞书通知验证器……"
/usr/bin/swift build -c release

echo ""
echo "即将监听飞书通知。首次运行时，请按系统提示授予辅助功能权限。"
echo "按 Control+C 可停止。记录文件：$PROBE_DIR/lark-notifications.jsonl"
echo ""

exec "$PROBE_DIR/.build/release/lark-notification-probe" \
  --prompt-permission \
  --output "$PROBE_DIR/lark-notifications.jsonl" \
  "$@"
