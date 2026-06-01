package com.yourteam.ingestion;

import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.yourteam.ingestion.config.ConfigLoader;
import com.yourteam.ingestion.config.JobConfig;
import com.yourteam.ingestion.config.RetryConfig;
import com.yourteam.ingestion.metrics.JsonEvents;
import com.yourteam.ingestion.metrics.StreamingMetricsListener;
import com.yourteam.ingestion.retry.RetryPolicy;
import com.yourteam.ingestion.retry.RetrySupervisor;
import com.yourteam.ingestion.transform.MessageTransform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the generic Spark -> Doris ingestion job.
 *
 * <pre>
 *   DORIS_PASSWORD=... java -jar spark-doris-ingestion.jar &lt;job-config.yaml&gt;
 * </pre>
 *
 * <p>Loads the YAML config, builds the SparkSession, reads the Kafka topic, maps each message
 * to the fixed Doris columns ({@link MessageTransform}), and streams the result into Doris.
 * Wiring lives in {@link IngestionPipeline}; this class is just the runnable shell.
 */
public final class IngestionJob {

    private static final Logger log = LoggerFactory.getLogger(IngestionJob.class);
    private static final String APP_NAME = "spark-doris-ingestion";

    private IngestionJob() {
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            log.error("Usage: java -jar spark-doris-ingestion.jar <job-config.yaml>");
            System.exit(2);
            return;
        }

        JobConfig config;
        String password;
        try {
            config = ConfigLoader.load(Paths.get(args[0]));
            password = config.getDoris().resolvePassword();
        } catch (RuntimeException e) {
            // Bad path / malformed YAML / missing field / unset password env — operator error.
            log.error("Configuration error: {}", e.getMessage());
            System.exit(2);
            return;
        }
        log.info("Loaded config: topic='{}' -> Doris {} via {}",
                config.getKafka().getTopic(),
                config.getDoris().tableIdentifier(),
                config.getDoris().getFenodes());

        SparkSession spark = IngestionPipeline.buildSession(config, APP_NAME);

        // Structured (JSON) metrics: one query_started, a batch_progress per micro-batch, and a
        // query_terminated event, all logged as JSON lines regardless of the active log backend.
        spark.streams().addListener(new StreamingMetricsListener());

        Map<String, Object> started = new LinkedHashMap<>();
        started.put("event", "job_started");
        started.put("app", APP_NAME);
        started.put("topic", config.getKafka().getTopic());
        started.put("table", config.getDoris().tableIdentifier());
        started.put("fenodes", config.getDoris().getFenodes());
        log.info(JsonEvents.toJson(started));

        // The query is recreated on each restart; the shutdown hook stops whichever one is active
        // so SIGTERM/Ctrl-C drains the current batch and leaves the checkpoint consistent. A clean
        // stop makes awaitTermination return normally, which ends the supervisor loop (no restart).
        AtomicReference<StreamingQuery> currentQuery = new AtomicReference<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — draining and stopping streaming query");
            StreamingQuery q = currentQuery.get();
            if (q != null) {
                try {
                    q.stop();
                } catch (Exception e) {
                    log.warn("Error while stopping query", e);
                }
            }
        }, "ingestion-shutdown"));

        try {
            Dataset<Row> kafka = IngestionPipeline.readKafkaStream(spark, config.getKafka());
            Dataset<Row> doris = MessageTransform.toDorisColumns(kafka);

            RetryConfig retryConfig = config.getRetry() != null ? config.getRetry() : RetryConfig.defaults();
            RetryPolicy policy = RetryPolicy.from(retryConfig);

            // Restart on transient failures with backoff; resume from the checkpoint each time.
            RetrySupervisor.run(() -> {
                StreamingQuery query = IngestionPipeline.dorisWriter(doris, config, password).start();
                currentQuery.set(query);
                log.info("Streaming started (output={}, trigger={}). Awaiting termination.",
                        config.getSpark().getOutputMode(), config.getSpark().getTrigger());
                query.awaitTermination();
            }, policy);
        } catch (Exception e) {
            log.error("Ingestion job failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}
