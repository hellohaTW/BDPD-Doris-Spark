package com.yourteam.ingestion.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step A — Spark only. Builds a DataFrame matching the exact Kafka source schema, runs
 * {@link MessageTransform}, and asserts the seven Doris columns. Proves Spark runs in-JVM
 * (provided deps are on the test classpath) and that the transform is correct.
 */
class MessageTransformTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("MessageTransformTest")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    /** The exact schema Spark's Kafka source produces when includeHeaders=true. */
    private static StructType kafkaSourceSchema() {
        StructType headerStruct = new StructType(new StructField[]{
                new StructField("key", DataTypes.StringType, true, Metadata.empty()),
                new StructField("value", DataTypes.BinaryType, true, Metadata.empty()),
        });
        return new StructType(new StructField[]{
                new StructField("key", DataTypes.BinaryType, true, Metadata.empty()),
                new StructField("value", DataTypes.BinaryType, true, Metadata.empty()),
                new StructField("topic", DataTypes.StringType, true, Metadata.empty()),
                new StructField("partition", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("offset", DataTypes.LongType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("timestampType", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("headers", DataTypes.createArrayType(headerStruct), true, Metadata.empty()),
        });
    }

    @Test
    void mapsKafkaRowToFixedDorisColumns() {
        Row header = RowFactory.create("trace-id", "abc-123".getBytes(StandardCharsets.UTF_8));
        Row kafkaRow = RowFactory.create(
                "order-42".getBytes(StandardCharsets.UTF_8),                 // key
                "{\"id\":42}".getBytes(StandardCharsets.UTF_8),              // value
                "orders",                                                    // topic
                3,                                                           // partition
                100L,                                                        // offset
                Timestamp.valueOf("2026-05-29 10:00:00"),                    // timestamp
                0,                                                           // timestampType
                Collections.singletonList(header));                         // headers

        Dataset<Row> kafka = spark.createDataFrame(
                Collections.singletonList(kafkaRow), kafkaSourceSchema());

        Dataset<Row> doris = MessageTransform.toDorisColumns(kafka);

        // Exactly the seven Doris columns, in DDL order.
        List<String> expectedCols = Arrays.asList(
                "kafka_timestamp", "kafka_partition", "kafka_offset",
                "kafka_key", "kafka_value", "kafka_headers", "ingestion_time");
        assertEquals(expectedCols, Arrays.asList(doris.columns()));

        Row out = doris.collectAsList().get(0);
        int partition = out.getAs("kafka_partition");
        long offset = out.getAs("kafka_offset");
        assertEquals(3, partition);
        assertEquals(100L, offset);
        assertEquals("order-42", out.<String>getAs("kafka_key"));
        assertEquals("{\"id\":42}", out.<String>getAs("kafka_value"));

        String headers = out.getAs("kafka_headers");
        assertTrue(headers.contains("\"key\":\"trace-id\""), "headers JSON: " + headers);
        assertTrue(headers.contains("\"value\":\"abc-123\""), "headers JSON: " + headers);

        // kafka_timestamp preserved; ingestion_time populated by the transform.
        assertEquals(Timestamp.valueOf("2026-05-29 10:00:00"), out.<Timestamp>getAs("kafka_timestamp"));
        assertNotNull(out.<Timestamp>getAs("ingestion_time"));
    }
}
