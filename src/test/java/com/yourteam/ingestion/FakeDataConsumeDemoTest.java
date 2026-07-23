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

/**
 * Demo / smoke test: publish a handful of fake events to an in-JVM Kafka broker, consume them
 * through the real Spark Kafka source + {@link MessageTransform}, then PRINT every row so you
 * can eyeball what the job actually reads from Kafka. This is the "run it and see" check until
 * Task 3 wires the real {@code IngestionJob.main}.
 *
 * <p>Run just this one:
 * <pre>source dev-env.sh && mvn -B -Dtest=FakeDataConsumeDemoTest test</pre>
 */
class FakeDataConsumeDemoTest {

    private static final String TOPIC = "fake-orders";

    private static SparkSession spark;
    private static EmbeddedKafkaConfig kafkaConfig;
    private static String bootstrap;

    @BeforeAll
    static void startInfra() {
        kafkaConfig = EmbeddedKafkaConfig$.MODULE$.defaultConfig();
        EmbeddedKafka$.MODULE$.start(kafkaConfig);
        bootstrap = "localhost:" + kafkaConfig.kafkaPort();

        spark = SparkSession.builder()
                .appName("FakeDataConsumeDemoTest")
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
        EmbeddedKafka$.MODULE$.stop();
    }

    private static void publish(String key, String value, Header... headers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(
                    TOPIC, null, key, value, java.util.Arrays.asList(headers)));
            producer.flush();
        }
    }

    @Test
    void consumeFakeDataFromKafkaAndPrint() throws Exception {
        // ---- 1. Publish fake events to Kafka (each with a trace-id header) ----
        publish("order-1001", "{\"orderId\":1001,\"item\":\"coffee\",\"qty\":2}",
                new RecordHeader("trace-id", "t-aaa".getBytes(StandardCharsets.UTF_8)));
        publish("order-1002", "{\"orderId\":1002,\"item\":\"tea\",\"qty\":5}",
                new RecordHeader("trace-id", "t-bbb".getBytes(StandardCharsets.UTF_8)));
        publish("order-1003", "{\"orderId\":1003,\"item\":\"bagel\",\"qty\":1}",
                new RecordHeader("trace-id", "t-ccc".getBytes(StandardCharsets.UTF_8)));

        // ---- 2. Consume from Kafka through the real Spark source + transform ----
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
                .queryName("fake_out")
                .outputMode("append")
                .start();
        try {
            query.processAllAvailable();
        } finally {
            query.stop();
        }

        // ---- 3. PRINT what we consumed ----
        Dataset<Row> result = spark.sql("SELECT * FROM fake_out ORDER BY kafka_key");

        System.out.println("\n================= CONSUMED FROM KAFKA (" + TOPIC + ") =================");
        // Pretty table (the seven fixed Doris columns):
        result.show(false);

        // Field-by-field, so it is unambiguous what each row carries:
        List<Row> rows = result.collectAsList();
        for (Row r : rows) {
            System.out.println("---- row ----");
            System.out.println("  kafka_timestamp : " + r.<Object>getAs("kafka_timestamp"));
            System.out.println("  kafka_partition : " + r.<Object>getAs("kafka_partition"));
            System.out.println("  kafka_offset    : " + r.<Object>getAs("kafka_offset"));
            System.out.println("  kafka_key       : " + r.<String>getAs("kafka_key"));
            System.out.println("  kafka_value     : " + r.<String>getAs("kafka_value"));
            System.out.println("  kafka_headers   : " + r.<String>getAs("kafka_headers"));
            System.out.println("  ingestion_time  : " + r.<Object>getAs("ingestion_time"));
        }
        System.out.println("======================================================================\n");

        // A light assertion so the demo also functions as a smoke test.
        assertEquals(3, rows.size());
        assertEquals(
                Collections.singletonList("order-1001"),
                Collections.singletonList(rows.get(0).<String>getAs("kafka_key")));
    }
}
