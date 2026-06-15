package com.yourteam.ingestion;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the fake-load transform. Feeds a DataFrame shaped like the {@code rate} source
 * ({@code timestamp}, {@code value}) into {@link FakeLoadJob#fakeColumns} and asserts the fixed
 * 7-column contract and that the generated content is well-formed — no Spark streaming, no Doris.
 */
class FakeLoadJobTest {

    private static SparkSession spark;

    @BeforeAll
    static void start() {
        spark = SparkSession.builder()
                .appName("FakeLoadJobTest").master("local[2]")
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
    void producesTheFixedSevenColumnsWithFakeContent() {
        // A rate-source-shaped frame: a monotonic `value` plus a `timestamp`.
        Dataset<Row> rateLike = spark.range(0, 25).toDF("value")
                .withColumn("timestamp", functions.current_timestamp());

        Dataset<Row> out = FakeLoadJob.fakeColumns(rateLike);

        assertEquals(
                Arrays.asList("kafka_timestamp", "kafka_partition", "kafka_offset",
                        "kafka_key", "kafka_value", "kafka_headers", "ingestion_time"),
                Arrays.asList(out.schema().fieldNames()));

        List<Row> rows = out.orderBy("kafka_offset").collectAsList();
        assertEquals(25, rows.size());

        Row first = rows.get(0);
        assertEquals(0L, first.<Long>getAs("kafka_offset"));               // offset == rate value
        int partition = first.<Integer>getAs("kafka_partition");
        assertTrue(partition >= 0 && partition <= 9, "partition in [0,9]: " + partition);
        assertTrue(first.<String>getAs("kafka_key").startsWith("order-"),
                first.<String>getAs("kafka_key"));

        String value = first.getAs("kafka_value");
        assertTrue(value.contains("\"id\":0") && value.contains("\"item\":"), "value JSON: " + value);

        String headers = first.getAs("kafka_headers");
        assertTrue(headers.contains("trace-id"), "headers JSON: " + headers);

        // partitions spread across buckets as value grows (pmod 10).
        long distinctPartitions = rows.stream()
                .map(r -> r.<Integer>getAs("kafka_partition")).distinct().count();
        assertTrue(distinctPartitions > 1, "expected several partitions, got " + distinctPartitions);
    }
}
