package com.yourteam.ingestion;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step B — embedded Kafka end-to-end. Starts an in-JVM Kafka broker, publishes records
 * with headers, reads them through Spark's real Kafka source, runs {@link MessageTransform},
 * and writes to a {@code memory} sink (a Doris stand-in) to assert the rows. Proves the
 * Kafka source path works with no Docker and no real Doris.
 */
class KafkaToDorisStreamTest {

    private static final String TOPIC = "orders";

    private static SparkSession spark;
    private static EmbeddedKafkaConfig kafkaConfig;
    private static String bootstrap;

    @BeforeAll
    static void startInfra() {
        kafkaConfig = EmbeddedKafkaConfig$.MODULE$.defaultConfig();
        EmbeddedKafka$.MODULE$.start(kafkaConfig);          // starts ZK + Kafka in-process
        bootstrap = "localhost:" + kafkaConfig.kafkaPort(); // default 6001

        spark = SparkSession.builder()
                .appName("KafkaToDorisStreamTest")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopInfra() {
        if (spark != null) {
            spark.stop();
        }
        // A harmless ZooKeeper EndOfStreamException may appear here at teardown.
        EmbeddedKafka$.MODULE$.stop();
    }

    private static void publish(String key, String value, Header... headers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC, null, key, value, java.util.Arrays.asList(headers));
            producer.send(record);
            producer.flush();
        }
    }

    @Test
    void readsFromKafkaThroughTransformIntoSink() throws Exception {
        Header traceHeader = new RecordHeader("trace-id", "abc-123".getBytes(StandardCharsets.UTF_8));
        publish("order-1", "{\"id\":1}", traceHeader);
        publish("order-2", "{\"id\":2}", traceHeader);

        Dataset<Row> kafka = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "earliest")
                .option("includeHeaders", "true")
                .load();

        Dataset<Row> doris = MessageTransform.toDorisColumns(kafka);

        StreamingQuery query = doris.writeStream()
                .format("memory")
                .queryName("doris_out")
                .outputMode("append")
                .start();
        try {
            query.processAllAvailable();
        } finally {
            query.stop();
        }

        List<Row> rows = spark.sql(
                "SELECT * FROM doris_out ORDER BY kafka_key").collectAsList();
        assertEquals(2, rows.size());

        // Exactly the seven Doris columns survive the full Kafka -> Spark -> sink path.
        assertEquals(
                java.util.Arrays.asList(
                        "kafka_timestamp", "kafka_partition", "kafka_offset",
                        "kafka_key", "kafka_value", "kafka_headers", "ingestion_time"),
                java.util.Arrays.asList(rows.get(0).schema().fieldNames()));

        assertEquals("order-1", rows.get(0).getAs("kafka_key"));
        assertEquals("{\"id\":1}", rows.get(0).getAs("kafka_value"));
        assertEquals("order-2", rows.get(1).getAs("kafka_key"));

        String headers = rows.get(0).getAs("kafka_headers");
        assertTrue(headers.contains("trace-id") && headers.contains("abc-123"),
                "headers JSON: " + headers);
    }

    /** Kept to silence "unused" on the single-arg helper when no headers are needed. */
    @SuppressWarnings("unused")
    private static void publish(String key, String value) {
        publish(key, value, new Header[0]);
    }
}
