package com.yourteam.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the generic Spark -> Doris ingestion job.
 *
 * <p>Placeholder main. Task 3 wires this up: load YAML config -> build SparkSession
 * (apply {@code spark.extra_conf}) -> Kafka {@code readStream} (with
 * {@code includeHeaders=true}) -> {@link com.yourteam.ingestion.transform.MessageTransform}
 * -> Doris {@code writeStream} -> checkpoint/trigger/output mode + shutdown hook.
 */
public final class IngestionJob {

    private static final Logger log = LoggerFactory.getLogger(IngestionJob.class);

    private IngestionJob() {
    }

    public static void main(String[] args) {
        log.info("spark-doris-ingestion starting (placeholder main). args={}", (Object) args);
        // TODO Task 3: config load -> SparkSession -> Kafka readStream -> transform -> Doris writeStream
        log.info("Nothing to run yet — see docs/DESIGN.md for the Task plan.");
    }
}
