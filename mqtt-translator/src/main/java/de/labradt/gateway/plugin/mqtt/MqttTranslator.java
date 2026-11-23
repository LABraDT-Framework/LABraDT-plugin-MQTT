package de.labradt.gateway.plugin.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.labradt.gateway.framework.model.AdapterMapping;
import de.labradt.gateway.framework.model.ProtocolType;
import de.labradt.gateway.framework.model.RawValue;
import de.labradt.gateway.framework.model.SubmodelElement;
import de.labradt.gateway.framework.translation.ITranslationLayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * MQTT translation layer. The adapter hands over the raw payload string
 * untouched; this class owns the wire semantics:
 *
 * <p><strong>Read:</strong> parses the JSON envelope
 * {@code {"value": ..., "timestamp": "...", "quality": "GOOD"}} (falling back to
 * treating the whole payload as a bare scalar if it isn't JSON), then runs the
 * same semantic pipeline as the OPC UA translator: value-map, unit
 * conversion, range validation.
 *
 * <p><strong>Write:</strong> reverses the value-map and unit conversion, casts
 * to the configured type, and wraps the result back into the JSON envelope so
 * the published message matches what the machine-side publisher produces.
 */
public class MqttTranslator implements ITranslationLayer {

    private static final Logger log = LoggerFactory.getLogger(MqttTranslator.class);

    private final ObjectMapper json = new ObjectMapper();

    // Read path

    @Override
    public SubmodelElement translateValue(RawValue rawValue, AdapterMapping mapping) {
        String payload = rawValue.getValue() != null ? rawValue.getValue().toString() : null;

        // 1. Unwrap the envelope (or treat the payload as a bare scalar).
        String valueStr = payload;
        String statusCode = "OK";
        String inferredType = "string";
        // Default to arrival time; the publisher's own timestamp wins if present.
        LocalDateTime timestamp = rawValue.getTimestamp();

        JsonNode envelope = tryParse(payload);
        if (envelope != null && envelope.isObject() && envelope.has("value")) {
            JsonNode v = envelope.get("value");
            valueStr = v.isTextual() ? v.asText() : v.toString();
            inferredType = inferType(v);
            String quality = envelope.path("quality").asText("GOOD");
            statusCode = "GOOD".equalsIgnoreCase(quality) ? "OK" : "ERROR";
            String ts = envelope.path("timestamp").asText(null);
            if (ts != null) {
                try {
                    timestamp = LocalDateTime.parse(ts);
                } catch (Exception ignored) {
                    // unparseable publisher timestamp; keep arrival time
                }
            }
        } else if (envelope != null && !envelope.isObject()) {
            // Bare JSON scalar, e.g. "42.5" or "\"RUNNING\""
            valueStr = envelope.isTextual() ? envelope.asText() : envelope.toString();
            inferredType = inferType(envelope);
        }

        // 2. Resolve value type: config valueType takes priority.
        String valueType = (mapping.getValueType() != null && !mapping.getValueType().isBlank())
                ? mapping.getValueType()
                : inferredType;

        // 3. Value map.
        valueStr = applyValueMap(valueStr, mapping.getValueMap());

        // 4. Unit conversion: only when both source and target present and different.
        String sourceUnitOut = null;
        String targetUnitOut = null;
        if (shouldConvertUnit(mapping)) {
            Double converted = applyUnitConversion(valueStr,
                    mapping.getSourceUnit(), mapping.getTargetUnit());
            if (converted != null) {
                valueStr = String.valueOf(converted);
                sourceUnitOut = mapping.getSourceUnit();
                targetUnitOut = mapping.getTargetUnit();
            }
        }

        // 5. Range validation: overrides statusCode when value is parseable.
        double[] range = mapping.getValidRange();
        if (range != null && range.length == 2 && valueStr != null) {
            try {
                double d = Double.parseDouble(valueStr);
                statusCode = (d >= range[0] && d <= range[1]) ? "OK" : "OUT_OF_RANGE";
            } catch (NumberFormatException ex) {
                // not numeric; keep envelope-derived statusCode
            }
        }

        // Assemble the finished standard AAS Property. Nothing downstream
        // reshapes this; what leaves here is what the gateway serves.
        SubmodelElement element = new SubmodelElement();
        element.setIdShort(mapping.getIdShort());
        element.setCategory(mapping.getCategory());
        element.setSemanticId(mapping.getSemanticId());
        element.setValueType(toXsdType(valueType));
        element.setValue(valueStr);
        element.setRoutingKey(mapping.routingKey());
        element.setUnit(mapping.getUnit());
        element.setSourceUnit(sourceUnitOut);
        element.setTargetUnit(targetUnitOut);
        element.setTimestamp(timestamp);
        element.setStatusCode(statusCode);
        element.setDeadband(mapping.getDeadband());
        return element;
    }

    /**
     * Maps the config's short type names to the XSD types the AAS metamodel
     * (IDTA-01001) requires for Property.valueType.
     */
    static String toXsdType(String shortType) {
        if (shortType == null) return "xs:string";
        if (shortType.startsWith("xs:")) return shortType;
        switch (shortType) {
            case "float": return "xs:double";
            case "int":   return "xs:int";
            case "bool":  return "xs:boolean";
            default:      return "xs:string";
        }
    }

    // Write path

    @Override
    public RawValue reverseTranslateElement(SubmodelElement element, AdapterMapping mapping) {
        String valueStr = element.getValue();
        if (valueStr == null) {
            throw new IllegalArgumentException(
                    "Cannot write a null value to element \"" + element.getIdShort() + "\".");
        }

        // 1. Reverse value map.
        Map<String, String> valueMap = mapping.getValueMap();
        if (valueMap != null && !valueMap.isEmpty()) {
            String reversed = null;
            for (Map.Entry<String, String> e : valueMap.entrySet()) {
                if (e.getValue().equals(valueStr)) {
                    reversed = e.getKey();
                    break;
                }
            }
            if (reversed == null) {
                throw new IllegalArgumentException(
                        "Cannot reverse-map value \"" + element.getValue()
                                + "\" for element \"" + element.getIdShort()
                                + "\". No matching entry in valueMap.");
            }
            valueStr = reversed;
        }

        // 2. Reverse unit conversion (swap source/target).
        if (shouldConvertUnit(mapping)) {
            Double converted = applyUnitConversion(valueStr,
                    mapping.getTargetUnit(), mapping.getSourceUnit());
            if (converted != null) valueStr = String.valueOf(converted);
        }

        // 3. Build the JSON envelope with a typed value node (xs: names per IDTA-01001).
        ObjectNode envelope = JsonNodeFactory.instance.objectNode();
        String targetType = mapping.getValueType() != null ? mapping.getValueType() : "xs:string";
        try {
            switch (targetType) {
                case "xs:double", "xs:float": envelope.put("value", Double.parseDouble(valueStr)); break;
                case "xs:int":                envelope.put("value", Integer.parseInt(valueStr)); break;
                case "xs:long":               envelope.put("value", Long.parseLong(valueStr)); break;
                case "xs:boolean":            envelope.put("value", Boolean.parseBoolean(valueStr)); break;
                default:                      envelope.put("value", valueStr); break;
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    "Cannot convert value \"" + valueStr + "\" to type \""
                            + targetType + "\" for element \"" + element.getIdShort() + "\".");
        }
        envelope.put("timestamp", LocalDateTime.now().withNano(0).toString());
        envelope.put("quality", "GOOD");

        return new RawValue(
                mapping.getSourceAddress(),
                envelope.toString(),
                "MQTT_PAYLOAD",
                LocalDateTime.now(),
                mapping.getProtocol(),
                null
        );
    }

    // SPI

    @Override
    public boolean supportsProtocol(ProtocolType protocol) {
        return ProtocolType.MQTT.equals(protocol);
    }

    @Override
    public ProtocolType getProtocol() {
        return ProtocolType.MQTT;
    }

    // Helpers

    private JsonNode tryParse(String payload) {
        if (payload == null || payload.isBlank()) return null;
        try {
            return json.readTree(payload);
        } catch (Exception e) {
            return null; // not JSON; treat as bare string
        }
    }

    private static String inferType(JsonNode v) {
        if (v.isInt() || v.isLong()) return "int";
        if (v.isNumber())            return "float";
        if (v.isBoolean())           return "bool";
        return "string";
    }

    private String applyValueMap(String valueStr, Map<String, String> valueMap) {
        if (valueMap == null || valueMap.isEmpty() || valueStr == null) return valueStr;
        return valueMap.getOrDefault(valueStr, valueStr);
    }

    private boolean shouldConvertUnit(AdapterMapping mapping) {
        String src = mapping.getSourceUnit();
        String tgt = mapping.getTargetUnit();
        return src != null && tgt != null
                && !src.isBlank() && !tgt.isBlank()
                && !src.equals(tgt);
    }

    /**
     * Applies a supported unit conversion. Returns null if the value can't be
     * parsed as a double or the source-to-target pair is not supported (caller
     * keeps the original value unchanged in that case).
     */
    private Double applyUnitConversion(String valueStr, String src, String tgt) {
        if (valueStr == null) return null;
        double d;
        try {
            d = Double.parseDouble(valueStr);
        } catch (NumberFormatException ex) {
            return null;
        }

        String key = src + "->" + tgt;
        switch (key) {
            case "mm->m":      return d / 1000.0;
            case "m->mm":      return d * 1000.0;
            case "deg->rad":   return d * Math.PI / 180.0;
            case "rad->deg":   return d * 180.0 / Math.PI;
            case "rpm->rad/s": return d * Math.PI / 30.0;
            case "rad/s->rpm": return d * 30.0 / Math.PI;
            default:
                log.warn("Unsupported unit conversion: {}->{}", src, tgt);
                return null;
        }
    }
}
