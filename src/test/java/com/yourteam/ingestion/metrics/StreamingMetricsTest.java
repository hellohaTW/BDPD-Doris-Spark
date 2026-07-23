package com.yourteam.ingestion.metrics;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamingMetricsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(String json) {
        try {
            return MAPPER.readValue(json, Map.class);
        } catch (Exception e) {
            throw new RuntimeException("not valid JSON: " + json, e);
        }
    }

    @Test
    void queryStartedHasLifecycleFields() {
        Map<String, Object> m = parse(StreamingMetrics.queryStarted("id-1", "run-1", "orders"));
        assertEquals("query_started", m.get("event"));
        assertEquals("id-1", m.get("id"));
        assertEquals("run-1", m.get("run_id"));
        assertEquals("orders", m.get("name"));
    }

    @Test
    void batchProgressCarriesThroughputNumbers() {
        Map<String, Object> m = parse(StreamingMetrics.batchProgress(
                "orders", 7L, "2026-06-01T00:00:00.000Z", 1500L, 1200.5, 1100.0));
        assertEquals("batch_progress", m.get("event"));
        assertEquals(7, m.get("batch_id"));
        assertEquals(1500, m.get("num_input_rows"));
        assertEquals(1200.5, m.get("input_rows_per_second"));
        assertEquals(1100.0, m.get("processed_rows_per_second"));
    }

    @Test
    void batchProgressRendersNonFiniteRatesAsNull() {
        // Empty batch: Spark reports NaN rates — must serialise as null (valid JSON), not "NaN".
        String json = StreamingMetrics.batchProgress(
                "orders", 0L, "2026-06-01T00:00:00.000Z", 0L, Double.NaN, Double.NaN);
        assertTrue(json.contains("\"input_rows_per_second\":null"), json);

        Map<String, Object> m = parse(json);
        assertTrue(m.containsKey("input_rows_per_second"));
        assertNull(m.get("input_rows_per_second"));
        assertNull(m.get("processed_rows_per_second"));
    }

    @Test
    void queryTerminatedReportsExceptionOrNull() {
        Map<String, Object> failed = parse(
                StreamingMetrics.queryTerminated("id-1", "run-1", "boom: doris down"));
        assertEquals("query_terminated", failed.get("event"));
        assertEquals("boom: doris down", failed.get("exception"));

        Map<String, Object> clean = parse(
                StreamingMetrics.queryTerminated("id-1", "run-1", null));
        assertTrue(clean.containsKey("exception"));
        assertNull(clean.get("exception"));
    }

    @Test
    void finiteGuardNullsOutNanAndInfinity() {
        assertNull(JsonEvents.finite(Double.NaN));
        assertNull(JsonEvents.finite(Double.POSITIVE_INFINITY));
        assertNull(JsonEvents.finite(Double.NEGATIVE_INFINITY));
        assertEquals(Double.valueOf(3.5), JsonEvents.finite(3.5));
    }
}
