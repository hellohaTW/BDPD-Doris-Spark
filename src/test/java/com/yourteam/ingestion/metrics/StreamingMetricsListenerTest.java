package com.yourteam.ingestion.metrics;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link StreamingMetricsListener} on a real Spark query (rate source, no Kafka/Doris
 * needed): a capturing sink proves {@code onQueryProgress} reads the live Spark progress event
 * and produces {@code batch_progress} JSON, end to end.
 */
class StreamingMetricsListenerTest {

    private static SparkSession spark;

    @BeforeAll
    static void start() {
        spark = SparkSession.builder()
                .appName("StreamingMetricsListenerTest").master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stop() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void capturesBatchProgressJsonFromARealQuery() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        StreamingMetricsListener listener = new StreamingMetricsListener(events::add, events::add);
        spark.streams().addListener(listener);
        try {
            Dataset<Row> rate = spark.readStream().format("rate")
                    .option("rowsPerSecond", "5").load();
            StreamingQuery q = rate.writeStream().format("memory").queryName("metrics_out")
                    .outputMode("append").trigger(Trigger.ProcessingTime("200 milliseconds")).start();

            long deadline = System.currentTimeMillis() + 20_000;
            try {
                while (events.stream().noneMatch(e -> e.contains("\"event\":\"batch_progress\""))
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(200);
                }
            } finally {
                q.stop();
            }
        } finally {
            spark.streams().removeListener(listener);
        }

        assertTrue(events.stream().anyMatch(e -> e.contains("\"event\":\"query_started\"")),
                "expected a query_started event");
        String batch = events.stream()
                .filter(e -> e.contains("\"event\":\"batch_progress\""))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no batch_progress JSON captured: " + events));
        // It is valid JSON carrying the throughput fields we log.
        assertTrue(batch.contains("\"num_input_rows\":"), batch);
        assertTrue(batch.contains("\"processed_rows_per_second\":"), batch);
        assertFalse(batch.contains("NaN"), "rates must be JSON-safe (null, not NaN): " + batch);
    }
}
