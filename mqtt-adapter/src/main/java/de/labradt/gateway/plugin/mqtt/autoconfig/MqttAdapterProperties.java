package de.labradt.gateway.plugin.mqtt.autoconfig;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Like the OPC UA adapter, the MQTT adapter is driven entirely by the
 * per-machine instance JSON files (each carries its own {@code connection}
 * block with the broker URL). This property only toggles the plugin on/off.
 */
@ConfigurationProperties(prefix = "dt.adapter.mqtt")
public class MqttAdapterProperties {

    private boolean enabled = false;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
}
