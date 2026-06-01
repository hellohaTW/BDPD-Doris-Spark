package com.yourteam.ingestion;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.yourteam.ingestion.config.DorisConfig;
import com.yourteam.ingestion.config.JobConfig;
import com.yourteam.ingestion.config.KafkaConfig;
import com.yourteam.ingestion.config.SparkStreamingConfig;
import com.yourteam.ingestion.transform.MessageTransform;

import io.github.embeddedkafka.EmbeddedKafka$;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import io.github.embeddedkafka.EmbeddedKafkaConfig$;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 6 — end-to-end integration test. Runs the real production wiring
 * ({@link IngestionPipeline#buildSession} / {@link IngestionPipeline#readKafkaStream} /
 * {@link MessageTransform#toDorisColumns}, with a real checkpoint and trigger) from an embedded
 * Kafka broker into a {@link StubDorisSink} (no Docker, no real Doris). Asserts the fixed 7-column
 * contract / verbatim payload / header JSON end to end, and that a restart resumes from the
 * checkpoint without reprocessing — the no-data-loss property the retry supervisor relies on.
 */
class EndToEndIngestionTest {

    private static EmbeddedKafkaConfig kafkaConfig;
    private static String bootstrap;
    private static SparkSession spark;

    @BeforeAll
    static void startInfra() {
        kafkaConfig = EmbeddedKafkaConfig$.MODULE$.defaultConfig();
        EmbeddedKafka$.MODULE$.start(kafkaConfig);
        bootstrap = "localhost:" + kafkaConfig.kafkaPort();

        // Exercise the production session builder; spark.extra_conf carries the local master.
        JobConfig bootstrapConfig = JobConfig.builder()
                .kafka(KafkaConfig.builder().bootstrapServers(bootstrap).topic("ignored").build())
                .doris(DorisConfig.builder().fenodes("fe:8030").database("ods").table("t")
                        .user("u").passwordEnv("DORIS_PASSWORD").build())
                .spark(SparkStreamingConfig.builder().checkpointLocation("/tmp/ignored")
                        .extraConf(Map.of(
                                "spark.master", "local[2]",
                                "spark.ui.enabled", "false",
                                "spark.sql.shuffle.partitions", "1"))
                        .build())
                .build();
        spark = IngestionPipeline.buildSession(bootstrapConfig, "EndToEndIngestionTest");
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopInfra() {
        if (spark != null) {
            spark.stop();
        }
        EmbeddedKafka$.MODULE$.stop();
    }

    @BeforeEach
    void resetSink() {
        StubDorisSink.reset();
    }

    private static void publish(String topic, String key, String value, Header... headers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, null, key, value, Arrays.asList(headers)));
            producer.flush();
        }
    }

    /** Runs the production read+transform path into the stub Doris sink and drains it. */
    private static void runOnce(String topic, String checkpoint) throws Exception {
        KafkaConfig kafka = KafkaConfig.builder()
                .bootstrapServers(bootstrap).topic(topic).startingOffsets("earliest").build();
        Dataset<Row> source = IngestionPipeline.readKafkaStream(spark, kafka);
        Dataset<Row> doris = MessageTransform.toDorisColumns(source);
        StreamingQuery query = StubDorisSink.start(doris, checkpoint, Trigger.AvailableNow());
        try {
            query.awaitTermination();
        } finally {
            query.stop();
        }
    }

    @Test
    void fullProductionPathLandsRowsWithFixedContract(@TempDir Path tmp) throws Exception {
        String topic = "e2e-contract";
        Header trace = new RecordHeader("trace-id", "t-aaa".getBytes(StandardCharsets.UTF_8));
        publish(topic, "order-1", "{\"orderId\":1,\"item\":\"coffee\"}", trace);
        publish(topic, "order-2", "{\"orderId\":2,\"item\":\"tea\"}", trace);
        publish(topic, "order-3", "raw-non-json-payload", trace);

        runOnce(topic, tmp.resolve("ckpt").toString());

        List<Row> rows = StubDorisSink.WRITTEN;
        assertEquals(3, rows.size(), "all published records should land");

        // Exactly the seven Doris columns, in the fixed DDL order.
        assertEquals(
                Arrays.asList("kafka_timestamp", "kafka_partition", "kafka_offset",
                        "kafka_key", "kafka_value", "kafka_headers", "ingestion_time"),
                Arrays.asList(rows.get(0).schema().fieldNames()));

        // Payload is carried verbatim (JSON and non-JSON alike) — schema-agnostic.
        List<String> values = rows.stream()
                .sorted((a, b) -> Long.compare(a.getAs("kafka_offset"), b.getAs("kafka_offset")))
                .map(r -> r.<String>getAs("kafka_value")).toList();
        assertEquals(
                Arrays.asList("{\"orderId\":1,\"item\":\"coffee\"}",
                        "{\"orderId\":2,\"item\":\"tea\"}", "raw-non-json-payload"),
                values);

        // Headers render as clean JSON (not base64 bytes).
        String headers = rows.get(0).getAs("kafka_headers");
        assertTrue(headers.contains("trace-id") && headers.contains("t-aaa"), headers);

        // The exact option contract that would be handed to the Doris connector.
        DorisConfig doris = DorisConfig.builder().fenodes("fe:8030").database("ods")
                .table("orders_raw").user("ingest").passwordEnv("DORIS_PASSWORD").build();
        Map<String, String> opts = IngestionPipeline.dorisOptions(doris, "s3cret");
        assertEquals("ods.orders_raw", opts.get("doris.table.identifier"));
        assertEquals("fe:8030", opts.get("doris.fenodes"));
    }

    @Test
    void restartResumesFromCheckpointWithoutReprocessing(@TempDir Path tmp) throws Exception {
        String topic = "e2e-resume";
        String checkpoint = tmp.resolve("ckpt").toString();

        // Run 1: three records published, processed, query stops.
        publish(topic, "k1", "v1");
        publish(topic, "k2", "v2");
        publish(topic, "k3", "v3");
        runOnce(topic, checkpoint);
        assertEquals(3, StubDorisSink.WRITTEN.size(), "first run consumes all available");

        // Run 2: two new records; a fresh query on the SAME checkpoint must resume from offset 3.
        StubDorisSink.reset();
        publish(topic, "k4", "v4");
        publish(topic, "k5", "v5");
        runOnce(topic, checkpoint);

        List<String> secondRun = StubDorisSink.WRITTEN.stream()
                .map(r -> r.<String>getAs("kafka_value")).sorted().toList();
        assertEquals(Arrays.asList("v4", "v5"), secondRun,
                "resume must process only the new records — no loss, no reprocessing");
    }
}
