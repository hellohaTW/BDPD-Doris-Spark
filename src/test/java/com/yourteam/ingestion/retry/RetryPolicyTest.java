package com.yourteam.ingestion.retry;

import org.junit.jupiter.api.Test;

import com.yourteam.ingestion.config.RetryConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicyTest {

    @Test
    void backoffGrowsGeometricallyAndIsCappedAtMax() {
        // initial=100ms, max=1000ms, x2
        RetryPolicy p = new RetryPolicy(10, 100, 1000, 2.0, Long.MAX_VALUE);
        assertEquals(100, p.nextBackoffMillis());
        assertEquals(200, p.nextBackoffMillis());
        assertEquals(400, p.nextBackoffMillis());
        assertEquals(800, p.nextBackoffMillis());
        assertEquals(1000, p.nextBackoffMillis()); // 1600 capped to 1000
        assertEquals(1000, p.nextBackoffMillis()); // stays at the cap
    }

    @Test
    void canRetryUntilMaxRestartsReached() {
        RetryPolicy p = new RetryPolicy(2, 1, 1, 2.0, Long.MAX_VALUE);
        assertTrue(p.canRetry());
        p.nextBackoffMillis();           // restart 1
        assertTrue(p.canRetry());
        p.nextBackoffMillis();           // restart 2
        assertFalse(p.canRetry());       // budget exhausted
    }

    @Test
    void negativeMaxRestartsMeansUnlimited() {
        RetryPolicy p = new RetryPolicy(-1, 1, 1, 2.0, Long.MAX_VALUE);
        for (int i = 0; i < 1000; i++) {
            p.nextBackoffMillis();
        }
        assertTrue(p.canRetry());
    }

    @Test
    void healthyRunResetsTheRestartCounter() {
        RetryPolicy p = new RetryPolicy(5, 1, 1, 2.0, 10_000);
        p.nextBackoffMillis();
        p.nextBackoffMillis();
        assertEquals(2, p.restartCount());

        p.onRunFor(9_999);               // shorter than resetAfter -> no reset
        assertEquals(2, p.restartCount());

        p.onRunFor(10_000);              // >= resetAfter -> reset
        assertEquals(0, p.restartCount());
        assertTrue(p.canRetry());
    }

    @Test
    void fromConfigConvertsSecondsToMillis() {
        RetryConfig cfg = RetryConfig.builder()
                .maxRestarts(3)
                .initialBackoffSeconds(5)
                .maxBackoffSeconds(300)
                .backoffMultiplier(2.0)
                .resetAfterSeconds(600)
                .build();

        RetryPolicy p = RetryPolicy.from(cfg);
        assertEquals(5_000, p.nextBackoffMillis());   // 5s
        assertEquals(10_000, p.nextBackoffMillis());  // 10s
    }
}
