#!/usr/bin/env bash
# Start the ShotCatcher Collector.
# Usage: ./start.sh [--no-trades] [--tmux]
#
# Prereqs (first time):
#   pip install -r requirements.txt --break-system-packages

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SESSION="shotcatcher-collector"
NO_TRADES=false
USE_TMUX=false

for arg in "$@"; do
  case "$arg" in
    --no-trades) NO_TRADES=true ;;
    --tmux)      USE_TMUX=true ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

# --- Python path for shared DB managers ---
DM="$SCRIPT_DIR/../BinanceDataManagers"
export PYTHONPATH="$DM:$DM/order_data_manager:$DM/trades_manager:$DM/user_trades_manager:$DM/klines_manager:$DM/position_manager${PYTHONPATH:+:$PYTHONPATH}"

RUN_CMD="python3 main.py"
$NO_TRADES && RUN_CMD="$RUN_CMD --no-trades"

# ── tmux mode ─────────────────────────────────────────────────────────────────
if $USE_TMUX; then
    if ! command -v tmux &>/dev/null; then
        echo "ERROR: tmux not found. Install with: sudo apt install tmux"; exit 1
    fi
    if tmux has-session -t "$SESSION" 2>/dev/null; then
        echo "Session '$SESSION' already running. Attaching..."
        exec tmux attach -t "$SESSION"
    fi

    EXPORT_CMD="export PYTHONPATH=\"$PYTHONPATH\""

    tmux new-session -d -s "$SESSION" -n "collector" -c "$SCRIPT_DIR" -x 200 -y 50
    tmux set-option -t "$SESSION" -g history-limit 50000
    tmux send-keys -t "$SESSION:collector" "$EXPORT_CMD && $RUN_CMD" Enter

    echo ""
    $NO_TRADES && echo "  [trades disabled]"
    echo "Session '$SESSION' started."
    echo "  Ctrl+B [ — scroll  |  Ctrl+B d — detach"
    echo "  Stop: tmux kill-session -t $SESSION"
    echo ""
    exec tmux attach -t "$SESSION"
fi

# ── foreground mode ───────────────────────────────────────────────────────────
$NO_TRADES && echo "  [trades disabled]"
echo "Starting collector..."
cd "$SCRIPT_DIR"
exec $RUN_CMD
