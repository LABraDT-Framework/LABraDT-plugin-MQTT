package de.labradt.gateway.plugin.mqtt;

import de.labradt.gateway.framework.adapter.ICPPSAdapter;
import de.labradt.gateway.framework.model.AdapterMapping;
import de.labradt.gateway.framework.model.ConnectionConfig;
import de.labradt.gateway.framework.model.ProtocolType;
import de.labradt.gateway.framework.model.RawValue;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MQTT adapter: bridges the framework's pull-style {@link ICPPSAdapter} onto
 * MQTT's push model. Incoming messages keep an in-memory cache fresh, and
 * {@code readAddress()} returns the latest cached payload for a topic.
 *
 * <p>Because MQTT separates "connected to the broker" from "the machine is
 * alive", liveness is reported machine-wide via {@link #isSourceAlive()}: the
 * broker link must be up AND at least one message must have arrived from the
 * machine within {@code connection.quietWindowMs}. Reads always serve the
 * latest cached value; the staleness flag is applied at serving time based on
 * that machine-level liveness, so a steady signal on a living machine never
 * looks stale while a silent machine's elements all go STALE together. A
 * window of 0 disables the silence check (report-by-exception machines).
 *
 * <p>Subscriptions are scoped: either the machine's configured
 * {@code connection.topicFilter}, or exactly the topics named by its mappings.
 * Retained messages warm the cache immediately on subscribe.
 *
 * <p>Connection management: an initial-connect retry loop with exponential
 * backoff (Paho's automaticReconnect only engages after a first successful
 * connect), then Paho's automatic reconnect from there on; topics are
 * re-subscribed on every (re)connect.
 */
public class MqttAdapter implements ICPPSAdapter {

    private static final Logger log = LoggerFactory.getLogger(MqttAdapter.class);

    /** Delay between connect and the discovery snapshot, letting retained
     *  messages arrive first. */
    private static final long DISCOVERY_DUMP_DELAY_S = 3;

    private final String machineId;
    private final String brokerUrl;
    private final String username;
    private final String password;
    private final String topicFilter;        // nullable; fall back to mapped topics
    private final List<String> mappedTopics;
    private final long quietWindowMs;        // machine-level silence tolerance; 0 = never stale
    private final long initialBackoffMs;
    private final long maxBackoffMs;

    private final Map<String, RawValue> cache = new ConcurrentHashMap<>();

    private volatile MqttAsyncClient client;
    private volatile boolean connected = false;

    /** When ANY message from this machine last arrived. One message on one
     *  topic proves the publisher alive (machine-level liveness). */
    private volatile LocalDateTime lastMessageAt;

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final AtomicInteger connectAttempts = new AtomicInteger(0);

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "mqtt-adapter");
                t.setDaemon(true);
                return t;
            });

    public MqttAdapter(String machineId, ConnectionConfig conn, List<String> mappedTopics) {
        this.machineId = machineId;
        this.brokerUrl = conn.getEndpoint();
        this.username = conn.getUsername() == null ? "" : conn.getUsername();
        this.password = conn.getPassword() == null ? "" : conn.getPassword();
        this.topicFilter = (conn.getTopicFilter() == null || conn.getTopicFilter().isBlank())
                ? null : conn.getTopicFilter();
        this.mappedTopics = mappedTopics == null ? List.of() : List.copyOf(mappedTopics);
        this.quietWindowMs = conn.getQuietWindowMs();
        this.initialBackoffMs = conn.getReconnectInitialBackoffMs();
        this.maxBackoffMs = conn.getReconnectMaxBackoffMs();
    }

    // Lifecycle

    @Override
    public void connect() {
        try {
            MqttAsyncClient newClient = new MqttAsyncClient(
                    brokerUrl, "dt-gateway-" + machineId, new MemoryPersistence());
            newClient.setCallback(new AdapterCallback());
            this.client = newClient;
        } catch (Exception e) {
            log.error("MQTT client setup failed for machine '{}' (bad broker URL?): {}",
                    machineId, e.getMessage());
            return;
        }
        attemptConnect();
    }

    private void attemptConnect() {
        if (shuttingDown.get()) return;

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);   // covers drops AFTER the first success
        options.setCleanSession(true);
        options.setConnectionTimeout(5);
        if (!username.isBlank()) {
            options.setUserName(username);
            options.setPassword(password.toCharArray());
        }

        int n = connectAttempts.incrementAndGet();
        log.info("MQTT connect attempt #{} to {} (machine '{}')", n, brokerUrl, machineId);
        try {
            client.connect(options, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken token) {
                    // connectComplete() in the callback handles subscription.
                }

                @Override
                public void onFailure(IMqttToken token, Throwable e) {
                    // Paho's automaticReconnect does NOT engage when the FIRST
                    // connect fails, so retry ourselves with exponential backoff.
                    long backoff = nextBackoff(n);
                    log.warn("MQTT connect attempt #{} failed: {}, retrying in {} ms",
                            n, e == null ? "?" : e.getMessage(), backoff);
                    scheduler.schedule(MqttAdapter.this::attemptConnect,
                            backoff, TimeUnit.MILLISECONDS);
                }
            });
        } catch (Exception e) {
            long backoff = nextBackoff(n);
            log.warn("MQTT connect attempt #{} threw: {}, retrying in {} ms",
                    n, e.getMessage(), backoff);
            scheduler.schedule(this::attemptConnect, backoff, TimeUnit.MILLISECONDS);
        }
    }

    private long nextBackoff(int attemptNumber) {
        long delay = initialBackoffMs * (1L << Math.min(attemptNumber - 1, 30));
        return Math.min(delay, maxBackoffMs);
    }

    private class AdapterCallback implements MqttCallbackExtended {
        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            log.info("MQTT {} to {} for machine '{}'",
                    reconnect ? "reconnected" : "connected", serverURI, machineId);
            connected = true;
            connectAttempts.set(0);
            subscribeScoped();
        }

        @Override
        public void connectionLost(Throwable cause) {
            connected = false;
            log.warn("MQTT connection lost for machine '{}': {}, Paho will reconnect",
                    machineId, cause == null ? "?" : cause.getMessage());
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            LocalDateTime now = LocalDateTime.now();
            lastMessageAt = now;
            cache.put(topic, new RawValue(
                    topic, payload, "MQTT_PAYLOAD",
                    now, ProtocolType.MQTT, "GOOD"));
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) { }
    }

    /**
     * Subscribes to the machine's own namespace only: either the configured
     * topicFilter, or each mapped topic individually. (Never "#": on a shared
     * plant broker that would ingest every machine's traffic.)
     */
    private void subscribeScoped() {
        try {
            if (topicFilter != null) {
                client.subscribe(topicFilter, 1);
                log.info("Subscribed to filter '{}' on {} (machine '{}')",
                        topicFilter, brokerUrl, machineId);
            } else if (!mappedTopics.isEmpty()) {
                String[] topics = mappedTopics.toArray(new String[0]);
                int[] qos = new int[topics.length];
                java.util.Arrays.fill(qos, 1);
                client.subscribe(topics, qos);
                log.info("Subscribed to {} mapped topics on {} (machine '{}')",
                        topics.length, brokerUrl, machineId);
            } else {
                log.warn("Machine '{}' has no topicFilter and no mapped topics, nothing to subscribe to",
                        machineId);
                return;
            }
            scheduler.schedule(MqttAdapter.this::dumpDiscoveredTopicsToFile,
                    DISCOVERY_DUMP_DELAY_S, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("MQTT subscribe failed for machine '{}': {}", machineId, e.getMessage());
        }
    }

    @Override
    public void disconnect() {
        shuttingDown.set(true);
        scheduler.shutdownNow();
        try {
            if (client != null && client.isConnected()) {
                client.disconnect().waitForCompletion(2000);
            }
            log.info("Disconnected MQTT machine '{}'", machineId);
        } catch (Exception e) {
            log.warn("Error during MQTT disconnect: {}", e.getMessage());
        } finally {
            connected = false;
        }
    }

    // Read / Write

    @Override
    public RawValue readAddress(String topic) {
        RawValue cached = cache.get(topic);
        if (cached == null) {
            ensureConnected();
            throw new IllegalStateException(
                    "No message received yet on topic: " + topic
                            + " (is the publisher running and the topic retained?)");
        }
        // Reads always serve the latest cached value. Whether the machine is
        // still alive is judged machine-wide via isSourceAlive(), not per topic:
        // a steady signal on a living machine must never look stale.
        return cached;
    }

    @Override
    public List<RawValue> readBatchAddresses(List<String> topics) {
        List<RawValue> results = new ArrayList<>();
        for (String topic : topics) {
            results.add(readAddress(topic));
        }
        return results;
    }

    @Override
    public void writeAddress(String topic, Object value) {
        ensureConnected();
        try {
            MqttMessage message = new MqttMessage(
                    String.valueOf(value).getBytes(StandardCharsets.UTF_8));
            message.setQos(1);
            message.setRetained(true);
            client.publish(topic, message);
            log.debug("Published {} = {}", topic, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish to topic: " + topic, e);
        }
    }

    private void ensureConnected() {
        if (!connected || client == null) {
            throw new IllegalStateException(
                    "MQTT adapter for machine '" + machineId + "' is not connected to " + brokerUrl);
        }
    }

    // Discovery snapshot (wizard support)

    private void dumpDiscoveredTopicsToFile() {
        String safeId = machineId.replaceAll("[^a-zA-Z0-9._-]", "_");
        Path out = Paths.get("logs", "discovered-nodes-" + safeId + ".txt");
        try {
            if (out.getParent() != null) Files.createDirectories(out.getParent());

            List<String> topics = new ArrayList<>(cache.keySet());
            topics.sort(String::compareTo);

            StringBuilder sb = new StringBuilder();
            sb.append("# Discovered nodes (").append(cache.size())
              .append(") at ").append(LocalDateTime.now()).append("\n");
            sb.append("# Machine: ").append(machineId).append("\n");
            sb.append("# Endpoint: ").append(brokerUrl).append("\n");
            sb.append("# Format: NodeId | BrowseName | DataType | Value\n\n");

            for (String topic : topics) {
                RawValue v = cache.get(topic);
                // BrowseName = last topic segment; payload type isn't known
                // until the translator parses it, so report the wire format.
                String name = topic.substring(topic.lastIndexOf('/') + 1);
                String val = String.valueOf(v.getValue());
                if (val.length() > 80) val = val.substring(0, 77) + "...";
                sb.append(String.format("%-50s | %-40s | %-18s | %s%n",
                        topic, name, "MQTT_PAYLOAD", val));
            }

            Files.writeString(out, sb.toString());
            log.info("Wrote MQTT discovery snapshot ({} topics) to {}",
                    cache.size(), out.toAbsolutePath());
        } catch (Exception e) {
            log.warn("Failed to write MQTT discovery file: {}", e.getMessage());
        }
    }

    // SPI

    @Override
    public String getMachineId() {
        return machineId;
    }

    @Override
    public ProtocolType getProtocol() {
        return ProtocolType.MQTT;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    /**
     * Machine-level liveness: the broker link is up AND at least one message
     * from this machine arrived within the quiet window. A window of 0
     * disables the silence check (for report-by-exception machines that may
     * legitimately stay quiet indefinitely).
     */
    @Override
    public boolean isSourceAlive() {
        if (!connected) return false;
        if (quietWindowMs <= 0) return true;
        LocalDateTime last = lastMessageAt;
        if (last == null) return false; // connected but never heard the machine
        return Duration.between(last, LocalDateTime.now()).toMillis() <= quietWindowMs;
    }

    @Override
    public Map<String, RawValue> getCachedNodes() {
        return Collections.unmodifiableMap(cache);
    }

    @Override
    public void validateMappings(Collection<AdapterMapping> mappings) {
        for (AdapterMapping mapping : mappings) {
            String topic = mapping.getSourceAddress();
            if (!cache.containsKey(topic)) {
                log.warn("Config topic has no messages yet: {} ({}), check the publisher",
                        topic, mapping.getIdShort());
            } else {
                log.info("Config OK: {} -> {}", topic, mapping.getIdShort());
            }
        }
    }
}
