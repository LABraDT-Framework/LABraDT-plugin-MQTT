#!/usr/bin/env bash
# Launch the MQTT mock machine + broker for the demo, and stop them all on Ctrl+C.
#
#   ./run_mocks.sh
#
# Started:
#   Mosquitto broker  tcp://localhost:1883       (MQTT)
#   MQTT molder       publishes factory/molder-mqtt/<Signal>

ROOT="$(cd "$(dirname "$0")/.." && pwd)"   # mqtt-plugin project folder
cd "$(dirname "$0")" || exit 1

pids=()

cleanup() {
  echo
  echo "Stopping mock servers..."
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null
  done
  wait 2>/dev/null
  echo "All stopped."
  exit 0
}
trap cleanup INT TERM

echo "Starting mock servers (Ctrl+C to stop all)..."

# ── MQTT side ────────────────────────────────────────────────────────────────
if command -v mosquitto >/dev/null 2>&1; then
  if pgrep -x mosquitto >/dev/null 2>&1; then
    echo "  Broker  (mqtt 1883)    already running — reusing it"
  else
    mosquitto                       & pids+=($!); echo "  Broker  (mqtt 1883)    pid $!"
    sleep 1   # let the broker come up before the publisher connects
  fi
  python3 mqtt_molder_publisher.py  & pids+=($!); echo "  Molder  (mqtt publish) pid $!"
else
  echo "  (mosquitto not found — skipping MQTT broker + publisher)"
fi

echo
echo "All running. Now start the gateway with this plugin and the demo config in"
echo "  $ROOT/configs (see $ROOT/README.md)."
echo

wait
