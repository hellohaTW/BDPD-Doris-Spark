package com.yourteam.ingestion.retry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetrySupervisorTest {

    /** Records the requested backoffs instead of actually sleeping. */
    private final List<Long> sleeps = new ArrayList<>();
    private final RetrySupervisor.Sleeper recordingSleeper = sleeps::add;
    /** Constant clock — run durations are 0, so the reset-on-healthy-run path never fires here. */
    private final java.util.function.LongSupplier zeroClock = () -> 0L;

    @Test
    void restartsUntilTheRunSucceedsThenStops() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = new RetryPolicy(5, 10, 100, 2.0, Long.MAX_VALUE);

        RetrySupervisor.run(() -> {
            // Fail on the first two attempts, succeed (return normally) on the third.
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("transient");
            }
        }, policy, recordingSleeper, zeroClock);

        assertEquals(3, attempts.get());                 // 1 initial + 2 restarts
        assertEquals(List.of(10L, 20L), sleeps);          // backoff before each restart
    }

    @Test
    void rethrowsAfterExhaustingTheRestartBudget() {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = new RetryPolicy(3, 10, 10, 2.0, Long.MAX_VALUE);

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                RetrySupervisor.run(() -> {
                    attempts.incrementAndGet();
                    throw new RuntimeException("always down");
                }, policy, recordingSleeper, zeroClock));

        assertEquals("always down", ex.getMessage());
        assertEquals(4, attempts.get());                 // 1 initial + 3 restarts, then give up
        assertEquals(3, sleeps.size());
    }

    @Test
    void cleanFirstRunNeverSleeps() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        RetryPolicy policy = new RetryPolicy(5, 10, 100, 2.0, Long.MAX_VALUE);

        RetrySupervisor.run(attempts::incrementAndGet, policy, recordingSleeper, zeroClock);

        assertEquals(1, attempts.get());
        assertTrue(sleeps.isEmpty());
    }

    @Test
    void interruptionIsNotTreatedAsAFailureToRetry() {
        RetryPolicy policy = new RetryPolicy(5, 10, 100, 2.0, Long.MAX_VALUE);

        assertThrows(InterruptedException.class, () ->
                RetrySupervisor.run(() -> {
                    throw new InterruptedException("stop");
                }, policy, recordingSleeper, zeroClock));

        assertTrue(sleeps.isEmpty());                    // no backoff, no restart
        assertTrue(Thread.interrupted());                // interrupt flag re-raised (and cleared here)
    }
}
