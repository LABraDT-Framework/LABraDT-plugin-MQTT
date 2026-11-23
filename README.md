# mqtt-plugin

MQTT protocol plugin for the LABraDT gateway: an MQTT client (Eclipse Paho) with
quiet-window liveness, and the translator that turns the JSON envelope (or a bare value)
into AAS Properties.

## What is in here

| Path | What it is |
|---|---|
| `mqtt-adapter/` | `MqttAdapter` (implements `ICPPSAdapter`): broker connection with retry, topic cache, quiet-window liveness, retained writes |
| `mqtt-translator/` | `MqttTranslator` (implements `ITranslationLayer`): JSON envelope `{value, timestamp, quality}` or bare value, reverse path for writes |
| `mocks/` | Python mock injection molder that publishes over MQTT, plus `run_demo.sh` / `run_mocks.sh` (start a Mosquitto broker and the publisher) |
| `configs/` | Demo machine config for the mock (`molder-mqtt`) |
| [MODULES.md](MODULES.md) | Class-by-class reference of both modules |

## Build

The modules depend on `de.labradt:dt-gateway-framework:0.0.1-SNAPSHOT` from the gateway project. There is
no package registry yet, so install the gateway core into your local Maven repository first:

```bash
# in the gateway project
mvn install -DskipTests

# then here
mvn install
```

The modules have no automated tests yet.

## Use it with the gateway

The plugin registers itself through Spring Boot auto-configuration
(`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` in each
module). To use it, put `mqtt-adapter` and `mqtt-translator` (and their dependencies, e.g.
Eclipse Paho) on the gateway's classpath. In a local build:

1. Install the gateway core (`mvn install -DskipTests` in the gateway project).
2. Install this plugin (`mvn install` here).
3. Add both modules to the dependencies of `dt-gateway-app/pom.xml` in the gateway project
   and rebuild it:

```xml
<dependency>
	<groupId>de.labradt</groupId>
	<artifactId>mqtt-adapter</artifactId>
	<version>0.0.1-SNAPSHOT</version>
</dependency>
<dependency>
	<groupId>de.labradt</groupId>
	<artifactId>mqtt-translator</artifactId>
	<version>0.0.1-SNAPSHOT</version>
</dependency>
```

The adapter switch is already on in the gateway's default `application.properties`; set it
where a profile turns it off (the lab profile does):

```properties
dt.adapter.mqtt.enabled=true
```

Each MQTT machine is one JSON file with `"protocol": "MQTT"`; see `ADDING_MACHINES.md` in the
gateway project for the format.

## Demo with the mock

Needs a Mosquitto broker (`mosquitto` on the PATH) and Python 3:

```bash
cd mocks
pip install -r requirements.txt
./run_demo.sh
```

Then copy the demo config into the gateway's config folder and start the gateway with this
plugin, from the gateway project folder:

```bash
cp <path to this project>/configs/molder-mqtt.json dt-gateway-app/src/main/resources/configs/
mvn -pl dt-gateway-app spring-boot:run
```

Or point the gateway at this project's `configs/` folder instead (the gateway's wizard and lab
tools still work on its own folder):

```bash
mvn -pl dt-gateway-app spring-boot:run \
  -Dspring-boot.run.arguments="--dt.framework.config-path=file:<path to this project>/configs"
```

Kill only the publisher (`pkill -f mqtt_molder_publisher.py`) and, after the configured quiet
window, the gateway serves the non-constant `molder-mqtt` values with `statusCode: "STALE"`; restart it
and the flag clears.
