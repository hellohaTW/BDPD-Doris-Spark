package com.yourteam.ingestion;

import java.util.LinkedHashMap;
import java.util.Map;

import com.yourteam.ingestion.config.DorisConfig;
import com.yourteam.ingestion.config.JobConfig;
import com.yourteam.ingestion.config.KafkaConfig;
import com.yourteam.ingestion.transform.MessageTransform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;

/**
 * The pieces of the ingestion job, factored out of {@link IngestionJob#main} so each step is
 * unit-testable. The Kafka read path is exercised in-JVM by tests; the Doris option mapping
 * and trigger parsing are pure and asserted directly (a live Doris is never required).
 */
public final class IngestionPipeline {

    private IngestionPipeline() {
    }

    /** Builds the SparkSession, applying {@code spark.extra_conf}. Master comes from conf/spark-submit. */
    public static SparkSession buildSession(JobConfig config, String appName) {
        SparkSession.Builder builder = SparkSession.builder().appName(appName);
        for (Map.Entry<String, String> e : config.getSpark().getExtraConf().entrySet()) {
            builder = builder.config(e.getKey(), e.getValue());
        }
        return builder.getOrCreate();
    }

    /**
     * The Kafka source options: core subscribe/offset settings, {@code includeHeaders=true}, and
     * SASL/SCRAM auth for a secured cluster. Pure and insertion-ordered so it can be asserted
     * directly (the live read is exercised against an embedded broker in tests).
     */
    public static Map<String, String> kafkaOptions(KafkaConfig kafka) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("kafka.bootstrap.servers", kafka.getBootstrapServers());
        options.put("subscribe", kafka.getTopic());
        options.put("startingOffsets", kafka.getStartingOffsets());
        options.put("includeHeaders", "true");
        // SASL/SCRAM auth for a secured Kafka cluster.
        options.put("kafka.sasl.mechanism", "SCRAM-SHA-512");
        options.put("kafka.security.protocol", "SASL_PLAINTEXT");
        options.put("kafka.sasl.jaas.config", "");   // TODO: set the JAAS config (username/password)
        if (kafka.getMaxOffsetsPerTrigger() != null) {
            options.put("maxOffsetsPerTrigger", String.valueOf(kafka.getMaxOffsetsPerTrigger()));
        }
        return options;
    }

    /** Opens the Kafka streaming source, applying {@link #kafkaOptions(KafkaConfig)}. */
    public static Dataset<Row> readKafkaStream(SparkSession spark, KafkaConfig kafka) {
        DataStreamReader reader = spark.readStream().format("kafka");
        for (Map.Entry<String, String> e : kafkaOptions(kafka).entrySet()) {
            reader = reader.option(e.getKey(), e.getValue());
        }
        return reader.load();
    }

    /**
     * The options handed to the Doris writer: the four core connection options plus any
     * pass-through extras from config (extras may override the core ones). Insertion-ordered
     * so it is easy to read and assert.
     */
    public static Map<String, String> dorisOptions(DorisConfig doris, String password) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("doris.fenodes", doris.getFenodes());
        options.put("doris.table.identifier", doris.tableIdentifier());
        options.put("doris.user", doris.getUser());
        options.put("doris.password", password);
        options.putAll(doris.getOptions());
        return options;
    }

    /** Maps a config trigger string to a Spark {@link Trigger}; null/blank means Spark's default. */
    public static Trigger parseTrigger(String trigger) {
        if (trigger == null || trigger.trim().isEmpty()) {
            return null;
        }
        String t = trigger.trim();
        if (t.equalsIgnoreCase("once")) {
            return Trigger.Once();
        }
        if (t.equalsIgnoreCase("availableNow")) {
            return Trigger.AvailableNow();
        }
        return Trigger.ProcessingTime(t);
    }

    /**
     * Builds the Doris {@code writeStream} (format, options, output mode, checkpoint, trigger)
     * without starting it. Apply {@link MessageTransform#toDorisColumns} to the source first.
     */
    public static DataStreamWriter<Row> dorisWriter(Dataset<Row> doris, JobConfig config, String password) {
        DataStreamWriter<Row> writer = doris.writeStream()
                .format("doris")
                .outputMode(config.getSpark().getOutputMode())
                .option("checkpointLocation", config.getSpark().getCheckpointLocation());
        for (Map.Entry<String, String> e : dorisOptions(config.getDoris(), password).entrySet()) {
            writer = writer.option(e.getKey(), e.getValue());
        }
        Trigger trigger = parseTrigger(config.getSpark().getTrigger());
        return trigger == null ? writer : writer.trigger(trigger);
    }
}
