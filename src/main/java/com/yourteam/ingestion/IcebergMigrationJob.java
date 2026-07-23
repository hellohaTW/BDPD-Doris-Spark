package com.yourteam.ingestion;

import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import com.yourteam.ingestion.config.ConfigLoader;
import com.yourteam.ingestion.config.MigrationConfig;
import com.yourteam.ingestion.metrics.JsonEvents;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the one-shot Iceberg -&gt; Doris migration job.
 *
 * <pre>
 *   DORIS_PASSWORD=... spark-submit \
 *     --class com.yourteam.ingestion.IcebergMigrationJob \
 *     target/spark-doris-ingestion.jar &lt;migration-config.yaml&gt;
 * </pre>
 *
 * <p>Loads the YAML config, registers the Iceberg source catalog, reads the source table (schema
 * preserved verbatim), and writes it into Doris with {@code SaveMode.Overwrite} (full-table
 * replace). The target Doris table must already exist with a compatible schema — this job issues
 * no DDL. Batch, not streaming: it runs once and exits. Wiring lives in
 * {@link IcebergToDorisMigrationPipeline}; this class is just the runnable shell.
 */
public final class IcebergMigrationJob {

    private static final Logger log = LoggerFactory.getLogger(IcebergMigrationJob.class);
    private static final String APP_NAME = "iceberg-to-doris-migration";

    private IcebergMigrationJob() {
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            log.error("Usage: spark-submit --class {} <migration-config.yaml>", IcebergMigrationJob.class.getName());
            System.exit(2);
            return;
        }

        MigrationConfig config;
        String password;
        try {
            config = ConfigLoader.load(Paths.get(args[0]), MigrationConfig.class);
            password = config.getDoris().resolvePassword();
        } catch (RuntimeException e) {
            // Bad path / malformed YAML / missing field — operator error.
            log.error("Configuration error: {}", e.getMessage());
            System.exit(2);
            return;
        }
        log.info("Loaded config: Iceberg {} -> Doris {} via {}",
                config.getIceberg().fullTableName(),
                config.getDoris().tableIdentifier(),
                config.getDoris().getFenodes());
        if (password.isEmpty()) {
            log.warn("Doris password is empty (password_env='{}' is unset or not configured) — "
                    + "connecting with no password", config.getDoris().getPasswordEnv());
        }

        SparkSession spark = IcebergToDorisMigrationPipeline.buildSession(config, APP_NAME);

        Map<String, Object> started = new LinkedHashMap<>();
        started.put("event", "migration_started");
        started.put("app", APP_NAME);
        started.put("source", config.getIceberg().fullTableName());
        started.put("target", config.getDoris().tableIdentifier());
        started.put("fenodes", config.getDoris().getFenodes());
        started.put("mode", "overwrite");
        log.info(JsonEvents.toJson(started));

        long startNanos = System.nanoTime();
        try {
            Dataset<Row> source = IcebergToDorisMigrationPipeline.readIcebergTable(spark, config.getIceberg());
            IcebergToDorisMigrationPipeline.writeToDoris(source, config.getDoris(), password);

            Map<String, Object> done = new LinkedHashMap<>();
            done.put("event", "migration_completed");
            done.put("app", APP_NAME);
            done.put("source", config.getIceberg().fullTableName());
            done.put("target", config.getDoris().tableIdentifier());
            done.put("elapsed_ms", (System.nanoTime() - startNanos) / 1_000_000L);
            log.info(JsonEvents.toJson(done));
        } catch (Exception e) {
            log.error("Migration job failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}
