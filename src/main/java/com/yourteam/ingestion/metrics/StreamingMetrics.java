package com.yourteam.ingestion.metrics;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pure builders for the structured streaming events the job logs (Task 5). Kept free of Spark
 * types so the exact JSON is unit-testable with plain values; {@link StreamingMetricsListener}
 * is the thin adapter that pulls these values off the Spark events.
 */
public final class StreamingMetrics {

    private StreamingMetrics() {
    }

    /** Emitted once when the job's query starts. */
    public static String queryStarted(String id, String runId, String name) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("event", "query_started");
        m.put("id", id);
        m.put("run_id", runId);
        m.put("name", name);
        return JsonEvents.toJson(m);
    }

    /**
     * Per-batch throughput metrics. {@code inputRowsPerSecond}/{@code processedRowsPerSecond} may
     * be NaN on an empty batch — those serialise as {@code null}.
     */
    public static String batchProgress(String name, long batchId, String timestamp,
                                       long numInputRows,
                                       double inputRowsPerSecond, double processedRowsPerSecond) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("event", "batch_progress");
        m.put("name", name);
        m.put("batch_id", batchId);
        m.put("timestamp", timestamp);
        m.put("num_input_rows", numInputRows);
        m.put("input_rows_per_second", JsonEvents.finite(inputRowsPerSecond));
        m.put("processed_rows_per_second", JsonEvents.finite(processedRowsPerSecond));
        return JsonEvents.toJson(m);
    }

    /** Emitted when the query terminates; {@code exception} is null on a clean stop. */
    public static String queryTerminated(String id, String runId, String exception) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("event", "query_terminated");
        m.put("id", id);
        m.put("run_id", runId);
        m.put("exception", exception);
        return JsonEvents.toJson(m);
    }
}
