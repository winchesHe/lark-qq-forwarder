#!/bin/zsh

set -euo pipefail

CONTROL_DIR="${0:A:h}"
VENV_DIR="$CONTROL_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"
CONTROL_PORT="8765"
CONTROL_ARGS=("$@")

for ((index = 1; index <= ${#CONTROL_ARGS}; index++)); do
  case "${CONTROL_ARGS[index]}" in
    --port)
      if (( index == ${#CONTROL_ARGS} )); then
        print -u2 "--port 需要一个端口号"
        exit 2
      fi
      CONTROL_PORT="${CONTROL_ARGS[index + 1]}"
      ;;
    --port=*)
      CONTROL_PORT="${CONTROL_ARGS[index]#--port=}"
      ;;
  esac
done

cd "$CONTROL_DIR"

if [[ ! -x "$PYTHON" ]]; then
  /opt/homebrew/bin/python3.12 -m venv "$VENV_DIR"
  "$VENV_DIR/bin/pip" install -r "$CONTROL_DIR/requirements-qq.txt"
fi

if [[ ! -x "$CONTROL_DIR/.build/release/lark-notification-probe" ]]; then
  /usr/bin/swift build -c release
fi

"$PYTHON" "$CONTROL_DIR/control_plane.py" "${CONTROL_ARGS[@]}" &
CONTROL_PID=$!

cleanup() {
  if kill -0 "$CONTROL_PID" 2>/dev/null; then
    kill "$CONTROL_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

for attempt in {1..20}; do
  if /usr/bin/curl -fsS --max-time 1 "http://127.0.0.1:${CONTROL_PORT}/api/session" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

/usr/bin/open "http://127.0.0.1:${CONTROL_PORT}" >/dev/null 2>&1 || true
wait "$CONTROL_PID"
