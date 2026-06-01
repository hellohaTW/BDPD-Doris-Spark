package com.yourteam.ingestion.config;

import java.util.List;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Restart/retry policy for the streaming query (Task 4). All fields are optional and carry
 * sensible defaults, so a config without a {@code retry:} section keeps working unchanged.
 *
 * <p>When the streaming query terminates with an exception (e.g. a transient Doris/Kafka
 * connect or write failure), the job restarts it with exponential backoff instead of dying.
 * Because Structured Streaming resumes from the checkpoint, restarts do not lose data. A query
 * that ran healthily for at least {@link #resetAfterSeconds} before failing resets the restart
 * counter, so well-spaced blips never exhaust the budget.
 */
@Value
@Builder
@Jacksonized
public class RetryConfig {

    /** Max consecutive restarts before giving up. {@code -1} means unlimited. Default 5. */
    @Builder.Default
    int maxRestarts = 5;

    /** Backoff before the first restart, in seconds. Default 5. */
    @Builder.Default
    long initialBackoffSeconds = 5;

    /** Upper bound the backoff grows to, in seconds. Default 300 (5 min). */
    @Builder.Default
    long maxBackoffSeconds = 300;

    /** Multiplier applied to the backoff after each restart. Default 2.0. */
    @Builder.Default
    double backoffMultiplier = 2.0;

    /**
     * If the query ran at least this many seconds before failing, the restart counter resets
     * to zero (the failure is treated as isolated, not a crash loop). Default 600 (10 min).
     */
    @Builder.Default
    long resetAfterSeconds = 600;

    /** The default policy (used when the YAML omits the {@code retry:} section). */
    public static RetryConfig defaults() {
        return RetryConfig.builder().build();
    }

    void collectMissing(List<String> missing) {
        if (maxRestarts < -1) {
            missing.add("retry.max_restarts must be >= -1");
        }
        if (initialBackoffSeconds < 0) {
            missing.add("retry.initial_backoff_seconds must be >= 0");
        }
        if (maxBackoffSeconds < initialBackoffSeconds) {
            missing.add("retry.max_backoff_seconds must be >= retry.initial_backoff_seconds");
        }
        if (backoffMultiplier < 1.0) {
            missing.add("retry.backoff_multiplier must be >= 1.0");
        }
        if (resetAfterSeconds < 0) {
            missing.add("retry.reset_after_seconds must be >= 0");
        }
    }
}
