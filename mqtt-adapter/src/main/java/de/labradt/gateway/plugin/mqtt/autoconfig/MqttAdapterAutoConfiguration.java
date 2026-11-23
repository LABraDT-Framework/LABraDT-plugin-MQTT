package de.labradt.gateway.plugin.mqtt.autoconfig;

import de.labradt.gateway.framework.adapter.AdapterPool;
import de.labradt.gateway.framework.adapter.ICPPSAdapter;
import de.labradt.gateway.framework.model.ConnectionConfig;
import de.labradt.gateway.framework.model.MachineConfig;
import de.labradt.gateway.framework.model.MachineConfigLoader;
import de.labradt.gateway.framework.model.ProtocolType;
import de.labradt.gateway.plugin.mqtt.MqttAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.List;

@AutoConfiguration
@EnableConfigurationProperties(MqttAdapterProperties.class)
@ConditionalOnProperty(prefix = "dt.adapter.mqtt", name = "enabled", havingValue = "true")
public class MqttAdapterAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MqttAdapterAutoConfiguration.class);

    /**
     * One MQTT adapter per machine config whose protocol is MQTT, using the
     * connection block in that machine's own instance file. The broker URL is
     * the {@code connection.endpoint} (e.g. "tcp://localhost:1883").
     */
    @Bean
    public AdapterPool mqttAdapterPool(MachineConfigLoader configLoader) {
        List<ICPPSAdapter> adapters = new ArrayList<>();

        for (MachineConfig config : configLoader.loadAll()) {
            if (config.getProtocol() != ProtocolType.MQTT) continue;

            ConnectionConfig conn = config.getConnection();
            if (conn == null || conn.getEndpoint() == null || conn.getEndpoint().isBlank()) {
                log.warn("MQTT machine '{}' has no connection.endpoint (broker URL), skipping",
                        config.getMachineId());
                continue;
            }

            // Scope the subscription to this machine's own topics (or its
            // configured topicFilter), never the whole broker.
            List<String> topics = config.getMappings() == null ? List.of()
                    : config.getMappings().stream()
                        .map(m -> m.getSourceAddress())
                        .distinct()
                        .toList();

            log.info("Creating MQTT adapter for machine '{}' -> {} ({} topics)",
                    config.getMachineId(), conn.getEndpoint(), topics.size());
            MqttAdapter adapter = new MqttAdapter(config.getMachineId(), conn, topics);
            adapter.connect();
            adapters.add(adapter);
        }

        if (adapters.isEmpty()) {
            log.warn("MQTT adapter enabled but no MQTT machines found in config path");
        }
        return () -> adapters;
    }
}
