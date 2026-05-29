package com.yourteam.ingestion;

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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class IngestionPipelineTest {

    private static final String TOPIC = "pipe-orders";

    private static SparkSession spark;
    private static EmbeddedKafkaConfig kafkaConfig;
    private static JobConfig jobConfig;

    @BeforeAll
    static void startInfra() {
        kafkaConfig = EmbeddedKafkaConfig$.MODULE$.defaultConfig();
        EmbeddedKafka$.MODULE$.start(kafkaConfig);
        String bootstrap = "localhost:" + kafkaConfig.kafkaPort();

        Map<String, String> extraConf = new LinkedHashMap<>();
        extraConf.put("spark.master", "local[*]");
        extraConf.put("spark.ui.enabled", "false");
        extraConf.put("spark.sql.shuffle.partitions", "1");

        jobConfig = JobConfig.builder()
                .kafka(KafkaConfig.builder()
                        .bootstrapServers(bootstrap)
                        .topic(TOPIC)
                        .startingOffsets("earliest")
                        .build())
                .doris(DorisConfig.builder()
                        .fenodes("fe:8030").database("ods").table("orders_raw")
                        .user("root").passwordEnv("DORIS_PASSWORD")
                        .build())
                .spark(SparkStreamingConfig.builder()
                        .checkpointLocation("/tmp/ckpt/pipe")
                        .extraConf(extraConf)
                        .build())
                .build();

        // buildSession must honour spark.extra_conf (spark.master here makes the local run work).
        spark = IngestionPipeline.buildSession(jobConfig, "IngestionPipelineTest");
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopInfra() {
        if (spark != null) {
            spark.stop();
        }
        EmbeddedKafka$.MODULE$.stop();
    }

    private static void publish(String key, String value) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:" + kafkaConfig.kafkaPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC, key, value));
            producer.flush();
        }
    }

    @Test
    void readKafkaStreamConsumesThroughTransformToFixedColumns() throws Exception {
        publish("k-1", "{\"v\":1}");
        publish("k-2", "{\"v\":2}");
        publish("k-3", "{\"v\":3}");

        // Exercise the real production read path.
        Dataset<Row> kafka = IngestionPipeline.readKafkaStream(spark, jobConfig.getKafka());
        Dataset<Row> doris = MessageTransform.toDorisColumns(kafka);

        StreamingQuery query = doris.writeStream()
                .format("memory")           // Doris stand-in; Doris itself is exercised on a real cluster
                .queryName("pipe_out")
                .outputMode("append")
                .start();
        try {
            query.processAllAvailable();
        } finally {
            query.stop();
        }

        List<Row> rows = spark.sql("SELECT * FROM pipe_out ORDER BY kafka_key").collectAsList();
        assertEquals(3, rows.size());
        assertEquals(
                Arrays.asList("kafka_timestamp", "kafka_partition", "kafka_offset",
                        "kafka_key", "kafka_value", "kafka_headers", "ingestion_time"),
                Arrays.asList(rows.get(0).schema().fieldNames()));
        assertEquals("k-1", rows.get(0).<String>getAs("kafka_key"));
        assertEquals("{\"v\":3}", rows.get(2).<String>getAs("kafka_value"));
    }

    @Test
    void dorisOptionsMapsCoreConnectionPlusExtras() {
        DorisConfig doris = DorisConfig.builder()
                .fenodes("fe:8030").database("ods").table("orders_raw")
                .user("ingest").passwordEnv("DORIS_PASSWORD")
                .options(Collections.singletonMap("doris.sink.batch.size", "100000"))
                .build();

        Map<String, String> opts = IngestionPipeline.dorisOptions(doris, "s3cret");

        assertEquals("fe:8030", opts.get("doris.fenodes"));
        assertEquals("ods.orders_raw", opts.get("doris.table.identifier"));
        assertEquals("ingest", opts.get("doris.user"));
        assertEquals("s3cret", opts.get("doris.password"));
        assertEquals("100000", opts.get("doris.sink.batch.size"));
    }

    @Test
    void parseTriggerHandlesAllForms() {
        assertNull(IngestionPipeline.parseTrigger(null));
        assertNull(IngestionPipeline.parseTrigger("   "));
        assertEquals(Trigger.Once(), IngestionPipeline.parseTrigger("once"));
        assertEquals(Trigger.AvailableNow(), IngestionPipeline.parseTrigger("availableNow"));
        assertEquals(Trigger.ProcessingTime("30 seconds"),
                IngestionPipeline.parseTrigger("30 seconds"));
    }
}
