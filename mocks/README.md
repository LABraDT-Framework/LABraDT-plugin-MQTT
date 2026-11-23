# Mock machines

`mqtt_molder_publisher.py` is a mock injection-moulding machine that publishes its
signals over MQTT (`factory/molder-mqtt/<Signal>`, retained, JSON envelope) to a
Mosquitto broker on `localhost:1883`. It is the source for the demo config
`../configs/molder-mqtt.json`.

Run the following commands after your installation of python.

``` pip install -r requirements.txt ```

Start the broker and the publisher together (Ctrl+C stops both):

``` ./run_demo.sh ```

`run_mocks.sh` starts the same MQTT mocks.
