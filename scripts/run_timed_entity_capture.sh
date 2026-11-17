#!/bin/bash
# Run from the project root with the Spark environment set up as in the README.
set -euo pipefail

if [[ -f .env ]]; then
  set -a
  source ./.env
  set +a
fi

python_bin=.venv/bin/python
checkpoint=checkpoints/entity_stream
run_dir=checkpoints/timed_capture
live_messages="$run_dir/live_messages.jsonl"
mkdir -p "$run_dir"

# Reuse the live-only file on later runs so the producer remembers its IDs.
if [[ ! -e "$live_messages" ]]; then
  cp "${COLLECTED_MESSAGES_PATH:-outputs/collected_messages.jsonl}" "$live_messages"
fi

stream_pid=
producer_pid=
cleanup() {
  for pid in "$producer_pid" "$stream_pid"; do
    if [[ -n "$pid" ]]; then
      kill -TERM "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$python_bin" -u part1/entity_stream.py --checkpoint "$checkpoint" >"$run_dir/stream.log" 2>&1 &
stream_pid=$!
for attempt in {1..90}; do
  if rg -q 'Entity query started' "$run_dir/stream.log"; then
    break
  fi
  if ! kill -0 "$stream_pid" 2>/dev/null; then
    tail -30 "$run_dir/stream.log" >&2
    exit 1
  fi
  sleep 1
done
if ! rg -q 'Entity query started' "$run_dir/stream.log"; then
  tail -30 "$run_dir/stream.log" >&2
  exit 1
fi

"$python_bin" -u part1/producer.py --output "$live_messages" >"$run_dir/producer.log" 2>&1 &
producer_pid=$!
printf 'Timed capture run started at %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')"
"$python_bin" -u -m scripts.capture_entity_charts "$@"
