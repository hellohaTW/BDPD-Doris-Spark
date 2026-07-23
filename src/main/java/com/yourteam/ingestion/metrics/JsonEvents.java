package com.yourteam.ingestion.metrics;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serialises a small ordered field map to a compact, single-line JSON string for structured
 * logging. Backend-agnostic: the JSON is the log <em>message</em>, so it survives whichever
 * binding is active (logback locally, log4j2 on a cluster) and stays greppable/parseable.
 *
 * <p>Uses the Jackson already on the classpath (pinned to Spark's 2.15.2). Never throws — a log
 * line must not be able to fail the job.
 */
public final class JsonEvents {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonEvents() {
    }

    /** Compact JSON for the given fields; falls back to an error event rather than throwing. */
    public static String toJson(Map<String, Object> fields) {
        try {
            return MAPPER.writeValueAsString(fields);
        } catch (JsonProcessingException e) {
            return "{\"event\":\"json_serialization_error\",\"error\":\""
                    + String.valueOf(e.getMessage()).replace('"', '\'') + "\"}";
        }
    }

    /**
     * Returns {@code v} boxed, or {@code null} if it is NaN/Infinite — Spark reports those for
     * rates on empty batches, and non-finite doubles are not valid JSON.
     */
    public static Double finite(double v) {
        return Double.isFinite(v) ? v : null;
    }
}
