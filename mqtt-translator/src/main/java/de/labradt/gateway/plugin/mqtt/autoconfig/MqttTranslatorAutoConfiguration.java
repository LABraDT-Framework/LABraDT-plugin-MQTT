package de.labradt.gateway.plugin.mqtt.autoconfig;

import de.labradt.gateway.plugin.mqtt.MqttTranslator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class MqttTranslatorAutoConfiguration {

    @Bean
    public MqttTranslator mqttTranslator() {
        return new MqttTranslator();
    }
}
