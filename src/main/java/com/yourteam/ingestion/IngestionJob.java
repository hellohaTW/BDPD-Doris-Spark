package com.yourteam.ingestion;

import java.nio.file.Paths;

import com.yourteam.ingestion.config.ConfigLoader;
import com.yourteam.ingestion.config.JobConfig;
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
        try {
            Dataset<Row> kafka = IngestionPipeline.readKafkaStream(spark, config.getKafka());
            Dataset<Row> doris = MessageTransform.toDorisColumns(kafka);
            StreamingQuery query = IngestionPipeline.dorisWriter(doris, config, password).start();

            // Stop the query cleanly on SIGTERM/Ctrl-C so the checkpoint is left consistent.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received — stopping streaming query");
                try {
                    query.stop();
                } catch (Exception e) {
                    log.warn("Error while stopping query", e);
                }
            }, "ingestion-shutdown"));

            log.info("Streaming started (output={}, trigger={}). Awaiting termination.",
                    config.getSpark().getOutputMode(), config.getSpark().getTrigger());
            query.awaitTermination();
        } catch (Exception e) {
            log.error("Ingestion job failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}
