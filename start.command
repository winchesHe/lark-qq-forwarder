#!/bin/zsh
set -euo pipefail
PROJECT_DIR="${0:A:h}"
if [[ -z "${LARK_QQ_STORAGE_DIR:-}" ]]; then
  export LARK_QQ_STORAGE_DIR="$HOME/Library/Application Support/lark-qq-forwarder"
else
  export LARK_QQ_STORAGE_DIR="${LARK_QQ_STORAGE_DIR:A}"
fi
mkdir -p "$LARK_QQ_STORAGE_DIR"
VENV_DIR="$PROJECT_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"
CONTROL_PORT="8765"
for ((index = 1; index <= $#; index++)); do
  case "${@[$index]}" in
    --port) (( index < $# )) || { print -u2 "--port 需要一个端口号"; exit 2; }; CONTROL_PORT="${@[$((index + 1))]}" ;;
    --port=*) CONTROL_PORT="${@[$index]#--port=}" ;;
    *) print -u2 "用法：$0 [--port 端口号]"; exit 2 ;;
  esac
done
cd "$PROJECT_DIR"
if [[ ! -x "$PYTHON" ]]; then
  /opt/homebrew/bin/python3.12 -m venv "$VENV_DIR"
  "$VENV_DIR/bin/pip" install -r "$PROJECT_DIR/requirements-qq.txt"
fi
if [[ ! -x "$PROJECT_DIR/.build/release/lark-notification-probe" ]]; then /usr/bin/swift build -c release; fi
"$PYTHON" "$PROJECT_DIR/control_plane.py" --port "$CONTROL_PORT" &
CONTROL_PID=$!
cleanup() { kill "$CONTROL_PID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM
for attempt in {1..20}; do
  if /usr/bin/curl -fsS --max-time 1 "http://127.0.0.1:${CONTROL_PORT}/api/session" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
/usr/bin/open "http://127.0.0.1:${CONTROL_PORT}" >/dev/null 2>&1 || true
wait "$CONTROL_PID"
