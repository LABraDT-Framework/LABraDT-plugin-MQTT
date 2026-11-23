#!/usr/bin/env bash
# Minimal demo: the one source for the demo config configs/molder-mqtt.json
# (broker + publisher on localhost). Ctrl+C stops everything.
#
#   ./run_demo.sh
#
#   MQTT machine      tcp://localhost:1883       (molder-mqtt: broker + publisher)

ROOT="$(cd "$(dirname "$0")/.." && pwd)"   # mqtt-plugin project folder
cd "$(dirname "$0")" || exit 1

pids=()

cleanup() {
  echo
  echo "Stopping mocks..."
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null
  done
  wait 2>/dev/null
  echo "All stopped."
  exit 0
}
trap cleanup INT TERM

echo "Starting demo mocks (Ctrl+C to stop all)..."

# ── MQTT side (broker + publisher = the one MQTT machine) ─────────────────────
if command -v mosquitto >/dev/null 2>&1; then
  if pgrep -x mosquitto >/dev/null 2>&1; then
    echo "  Broker  (mqtt 1883)    already running — reusing it"
  else
    mosquitto                       & pids+=($!); echo "  Broker  (mqtt 1883)    pid $!"
    sleep 1   # let the broker come up before the publisher connects
  fi
  python3 mqtt_molder_publisher.py  & pids+=($!); echo "  Molder  (mqtt publish) pid $!"
else
  echo "  (mosquitto not found — install with: brew install mosquitto)"
fi

echo
echo "All running. Start the gateway with this plugin (see $ROOT/README.md) in another"
echo "terminal, from the gateway project folder:"
echo "  mvn -pl dt-gateway-app spring-boot:run \\"
echo "    -Dspring-boot.run.arguments=\"--dt.framework.config-path=file:$ROOT/configs\""
echo

wait
