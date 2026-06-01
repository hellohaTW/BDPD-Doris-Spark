package com.yourteam.ingestion.retry;

import java.util.concurrent.TimeUnit;

import com.yourteam.ingestion.config.RetryConfig;

/**
 * Stateful exponential-backoff policy for restarting the streaming query. Pure logic — no Spark,
 * no clock, no sleeping — so the backoff maths, the give-up decision and the counter reset are
 * all unit-testable. One instance per job run; {@link RetrySupervisor} drives it.
 */
public final class RetryPolicy {

    private final int maxRestarts;
    private final long initialBackoffMillis;
    private final long maxBackoffMillis;
    private final double backoffMultiplier;
    private final long resetAfterMillis;

    /** Number of restarts performed since the last reset. */
    private int restartCount = 0;

    public RetryPolicy(int maxRestarts, long initialBackoffMillis, long maxBackoffMillis,
                       double backoffMultiplier, long resetAfterMillis) {
        this.maxRestarts = maxRestarts;
        this.initialBackoffMillis = initialBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
        this.backoffMultiplier = backoffMultiplier;
        this.resetAfterMillis = resetAfterMillis;
    }

    /** Builds a policy from config (seconds -> millis). */
    public static RetryPolicy from(RetryConfig cfg) {
        return new RetryPolicy(
                cfg.getMaxRestarts(),
                TimeUnit.SECONDS.toMillis(cfg.getInitialBackoffSeconds()),
                TimeUnit.SECONDS.toMillis(cfg.getMaxBackoffSeconds()),
                cfg.getBackoffMultiplier(),
                TimeUnit.SECONDS.toMillis(cfg.getResetAfterSeconds()));
    }

    /**
     * Records that the query ran for {@code runMillis} before terminating. A run that lasted at
     * least {@code resetAfterSeconds} is treated as healthy and resets the restart counter, so
     * failures spaced far apart never exhaust the budget.
     */
    public void onRunFor(long runMillis) {
        if (runMillis >= resetAfterMillis) {
            restartCount = 0;
        }
    }

    /** Whether another restart is allowed. {@code maxRestarts < 0} means unlimited. */
    public boolean canRetry() {
        return maxRestarts < 0 || restartCount < maxRestarts;
    }

    /**
     * The backoff to wait before the next restart, then advances the counter. Grows
     * geometrically from the initial value and is capped at the maximum.
     */
    public long nextBackoffMillis() {
        double grown = initialBackoffMillis * Math.pow(backoffMultiplier, restartCount);
        long capped = (long) Math.min(grown, (double) maxBackoffMillis);
        restartCount++;
        return capped;
    }

    /** Restarts performed since the last reset (for logging/tests). */
    public int restartCount() {
        return restartCount;
    }
}
