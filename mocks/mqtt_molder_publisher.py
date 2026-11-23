#!/usr/bin/env python3
"""
mqtt_molder_publisher.py — a mock injection-moulding machine that publishes
telemetry over MQTT, to prove the DT Gateway is protocol-agnostic (this is the
MQTT analog of the OPC UA molder mock injection_molder_server.py, which now lives in
the internal project gateway-plugins/opcua-plugin).

Requires a running MQTT broker (Mosquitto):
    brew install mosquitto
    mosquitto              # foreground on localhost:1883
    # or: brew services start mosquitto

Then:
    pip install paho-mqtt
    python3 mqtt_molder_publisher.py

Wire contract (consumed by mqtt-adapter / mqtt-translator):
    topic   : factory/<machineId>/<SignalName>
              e.g. factory/molder-mqtt/InjectionPressure
    payload : {"value": <number|string>, "timestamp": "<iso>", "quality": "GOOD"}
    retained: true  (so a late subscriber immediately gets each signal's last value)

So in a machine config, sourceAddress = the topic, and the translator pulls
".value" out of the JSON payload.
"""

import argparse
import json
import math
import signal
import sys
import time
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("paho-mqtt not installed — run:  pip install paho-mqtt")

# ── injection cycle (same shape as the OPC UA molder mock) ───────────────────
PHASE_IDLE, PHASE_INJECTING, PHASE_COOLING, PHASE_EJECTING = 0, 1, 2, 3
PHASE_LABELS = {0: "IDLE", 1: "INJECTING", 2: "COOLING", 3: "EJECTING"}
PHASE_DURATIONS = {PHASE_IDLE: 2.0, PHASE_INJECTING: 4.0, PHASE_COOLING: 8.0, PHASE_EJECTING: 2.0}
FULL_CYCLE = sum(PHASE_DURATIONS.values())


def current_phase(t):
    pos = t % FULL_CYCLE
    for phase in (PHASE_IDLE, PHASE_INJECTING, PHASE_COOLING, PHASE_EJECTING):
        if pos < PHASE_DURATIONS[phase]:
            return phase
        pos -= PHASE_DURATIONS[phase]
    return PHASE_IDLE


def compute_signals(t, phase, shot_count, cycle_elapsed):
    btemp1 = 220.0 + 3.0 * math.sin(t * 0.05)
    btemp2 = 230.0 + 2.5 * math.sin(t * 0.04 + 0.5)
    mtemp = 45.0 + 8.0 * math.sin(t * (2 * math.pi / FULL_CYCLE))

    if phase == PHASE_INJECTING:
        pressure, clamp, screw = 1600.0 + 200.0 * math.sin(t * 1.5), 2200.0, 5.0 + 3.0 * math.sin(t * 1.5)
    elif phase == PHASE_COOLING:
        pressure, clamp, screw = 800.0 + 50.0 * math.sin(t * 0.5), 2200.0, 120.0
    elif phase == PHASE_EJECTING:
        pressure, clamp, screw = 0.0, 100.0, 120.0 + 10.0 * math.sin(t * 0.3)
    else:  # IDLE
        pressure, clamp, screw = 0.0, 200.0, 120.0 + 10.0 * math.sin(t * 0.3)

    return {
        "MachineState": PHASE_LABELS[phase],            # already mapped to a label
        "BarrelTemperatureZone1": round(btemp1, 2),
        "BarrelTemperatureZone2": round(btemp2, 2),
        "MoldTemperature": round(mtemp, 2),
        "InjectionPressure": round(pressure, 2),
        "ClampForce": round(clamp, 2),
        "CycleTime": round(cycle_elapsed, 2),
        "ShotCount": shot_count,
        "MaterialType": "ABS",
        "PlasticizingTime": round(3.5 + 0.2 * math.sin(t * 0.3), 3),
        "ScrewPosition": round(screw, 2),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--machine-id", default="molder-mqtt")
    ap.add_argument("--base-topic", default="factory")
    args = ap.parse_args()

    base = f"{args.base_topic}/{args.machine_id}"

    client = mqtt.Client(client_id=f"mock-{args.machine_id}")
    try:
        client.connect(args.host, args.port, keepalive=30)
    except Exception as e:
        sys.exit(f"Could not connect to broker at {args.host}:{args.port} — is mosquitto running?  ({e})")
    client.loop_start()

    stop = False

    def _stop(_s, _f):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"Publishing molder telemetry to {base}/<Signal> on {args.host}:{args.port}")
    print("Press Ctrl+C to stop.")

    start = time.monotonic()
    shot_count = 0
    last_phase = -1
    cycle_start = 0.0

    while not stop:
        t = time.monotonic() - start
        phase = current_phase(t)
        if last_phase == PHASE_EJECTING and phase == PHASE_IDLE:
            shot_count += 1
            cycle_start = t
        if last_phase == -1:
            cycle_start = t
        last_phase = phase

        signals = compute_signals(t, phase, shot_count, t - cycle_start)
        ts = datetime.now().isoformat(timespec="seconds")

        for name, value in signals.items():
            payload = json.dumps({"value": value, "timestamp": ts, "quality": "GOOD"})
            # QoS 1 (at-least-once) — what real telemetry deployments use.
            client.publish(f"{base}/{name}", payload, qos=1, retain=True)

        time.sleep(1.0)

    client.loop_stop()
    client.disconnect()
    print("\nStopped.")


if __name__ == "__main__":
    main()
