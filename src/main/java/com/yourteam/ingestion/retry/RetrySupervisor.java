package com.yourteam.ingestion.retry;

import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a streaming query and restarts it on failure per a {@link RetryPolicy} (Task 4).
 *
 * <p>The query lifecycle, the sleep and the clock are injected as functional interfaces, so the
 * restart/backoff/give-up behaviour is exercised in-JVM without a real Spark query or Doris (in
 * keeping with the project's no-Docker validation approach). {@link com.yourteam.ingestion.IngestionJob}
 * passes the real query for production.
 */
public final class RetrySupervisor {

    private static final Logger log = LoggerFactory.getLogger(RetrySupervisor.class);

    /** Starts the query and blocks until it terminates; throws if it terminated with an error. */
    @FunctionalInterface
    public interface StreamRun {
        void runUntilTerminated() throws Exception;
    }

    /** Sleeps for the given number of millis (injected so tests need not actually wait). */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private RetrySupervisor() {
    }

    /** Production entry point: real wall clock and {@link Thread#sleep}. */
    public static void run(StreamRun run, RetryPolicy policy) throws Exception {
        run(run, policy, Thread::sleep, System::currentTimeMillis);
    }

    /**
     * Runs {@code run} until it completes normally (e.g. a graceful stop), restarting it with
     * backoff each time it throws, until the policy is exhausted — in which case the last failure
     * is rethrown for the caller to handle (non-zero exit). A normal return ends the loop.
     */
    public static void run(StreamRun run, RetryPolicy policy, Sleeper sleeper, LongSupplier clock)
            throws Exception {
        while (true) {
            long startedAt = clock.getAsLong();
            try {
                run.runUntilTerminated();
                return; // clean termination (graceful stop) — done, no restart.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e; // interruption is a stop signal, not a query failure — do not retry.
            } catch (Exception failure) {
                policy.onRunFor(clock.getAsLong() - startedAt);
                if (!policy.canRetry()) {
                    log.error("Streaming query failed and the restart budget is exhausted "
                            + "(restarts={}). Giving up.", policy.restartCount(), failure);
                    throw failure;
                }
                long backoff = policy.nextBackoffMillis();
                log.warn("Streaming query failed; restarting in {} ms (restart {}). Cause: {}",
                        backoff, policy.restartCount(), failure.toString());
                sleeper.sleep(backoff);
                // loop -> restart; Structured Streaming resumes from the checkpoint.
            }
        }
    }
}
