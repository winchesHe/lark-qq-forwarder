#!/bin/zsh

set -euo pipefail

FORWARDER_DIR="${0:A:h}"
VENV_DIR="$FORWARDER_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"

cd "$FORWARDER_DIR"

if [[ ! -x "$PYTHON" ]]; then
  /opt/homebrew/bin/python3.12 -m venv "$VENV_DIR"
  "$VENV_DIR/bin/pip" install -r "$FORWARDER_DIR/requirements-qq.txt"
fi

/usr/bin/swift build -c release
"$PYTHON" "$FORWARDER_DIR/qq_bridge.py" prime

PROBE_PID=""
BRIDGE_PID=""

cleanup() {
  [[ -n "$PROBE_PID" ]] && kill "$PROBE_PID" 2>/dev/null || true
  [[ -n "$BRIDGE_PID" ]] && kill "$BRIDGE_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

"$FORWARDER_DIR/.build/release/lark-notification-probe" \
  --prompt-permission \
  --output "$FORWARDER_DIR/lark-notifications.jsonl" &
PROBE_PID=$!

"$PYTHON" "$FORWARDER_DIR/qq_bridge.py" run &
BRIDGE_PID=$!

echo "飞书 → QQ 转发已启动。按 Control+C 停止。"

while kill -0 "$PROBE_PID" 2>/dev/null && kill -0 "$BRIDGE_PID" 2>/dev/null; do
  sleep 1
done

wait "$PROBE_PID" 2>/dev/null || true
wait "$BRIDGE_PID" 2>/dev/null || true
