# mqtt-plugin: module reference

Class-by-class reference of the two modules. It was moved here from the gateway project's
`SYSTEM_DOCUMENTATION.md` (sections 7 and 8) together with the code. The core it plugs into
(`dt-gateway-framework`: SPIs, registries, poll loop, REST/SSE) is described in that file.

---

## 1. Module: `mqtt-adapter`

**`MqttAdapter`** (implements `ICPPSAdapter`; built on Eclipse Paho)
Bridges the framework's pull-style adapter SPI onto MQTT's push model. One instance per MQTT machine; the "endpoint" is the broker URL.

*Cache-first reads.* Incoming messages (an `MqttCallbackExtended`) stamp `lastMessageAt` and store the raw payload string per topic in the cache. `readAddress(topic)` returns the cached payload; if no message has ever arrived on that topic it throws (with a hint to check the publisher and retained flag), which the poll loop records as a skip.

*Connection management.* Paho's `automaticReconnect` only engages after a first successful connect, so the adapter runs its own initial-connect retry loop with exponential backoff, then hands over to Paho. Topics are re-subscribed on every (re)connect via `connectComplete`.

*Scoped subscriptions.* Subscribes to either the machine's configured `topicFilter` or exactly the topics named by its mappings. Never `#`: on a shared plant broker that would ingest every machine's traffic.

*Machine-level liveness.* `isSourceAlive()` = broker link up AND at least one message from this machine within `quietWindowMs`. One message on any topic proves the publisher alive. Window 0 disables the check. This is the mechanism behind the whole-machine STALE behavior and the `QUIET` health state.

*Writes.* `writeAddress()` publishes the payload QoS 1 and **retained**, so late subscribers (including the gateway itself after a restart) immediately see the last value.

*Discovery.* Three seconds after subscribing (letting retained messages arrive first), dumps the topic cache to `logs/discovered-nodes-<machineId>.txt` in the same table format as the OPC UA adapter, for wizard support.

**`MqttAdapterAutoConfiguration`** + **`MqttAdapterProperties`**
Mirror image of the OPC UA autoconfig (in the opcua-plugin project): active on `dt.adapter.mqtt.enabled=true`, builds one adapter per `MQTT` machine config with that machine's connection block and distinct mapped topics, exposes them as an `AdapterPool`.

---

## 2. Module: `mqtt-translator`

**`MqttTranslator`** (implements `ITranslationLayer`)
Owns the MQTT wire semantics. The agreed wire contract is a JSON envelope per topic:

```json
{"value": 2200.0, "timestamp": "2026-07-11T10:15:00", "quality": "GOOD"}
```

*Read*: parses the envelope (falling back to treating the payload as a bare JSON scalar or raw string when it is not an object with `value`), infers the type from the JSON node when the config does not pin one, maps `quality` to `OK`/`ERROR`, prefers the publisher's own timestamp over arrival time (keeping arrival time if unparseable), then runs the **same semantic pipeline as the OPC UA translator**: value map, unit conversion (same supported pairs), range validation, assembly of the finished standard Property.

*Write*: reverse value map, reversed unit conversion, then builds a fresh JSON envelope with a **typed** value node (double/int/boolean/string per the configured xs: type), current timestamp, and `quality: "GOOD"`, so published writes are indistinguishable in shape from what the machine-side publisher produces.

**`MqttTranslatorAutoConfiguration`**: unconditional bean registration.

---

## 3. Mocks (`mocks/`)

- `mqtt_molder_publisher.py`: the MQTT mock injection molder. Publishes `factory/<machineId>/<SignalName>` with the JSON envelope, retained, against a local Mosquitto broker (`--host`, `--port`, `--machine-id`, `--base-topic`).
- `run_demo.sh`: starts the Mosquitto broker (if installed) and the publisher for `configs/molder-mqtt.json`, prints the gateway start command with a `file:` config-path override, and stops everything on Ctrl+C.
- `run_mocks.sh`: starts the same MQTT mocks.

## 4. Configuration

| Property | Default | Purpose |
|---|---|---|
| `dt.adapter.mqtt.enabled` | false | MQTT plugin on/off (all else per machine file) |

Each MQTT machine is one JSON file with `"protocol": "MQTT"`; its `connection.endpoint` is the broker URL (e.g. `tcp://localhost:1883`) and each mapping's `sourceAddress` is a topic.
