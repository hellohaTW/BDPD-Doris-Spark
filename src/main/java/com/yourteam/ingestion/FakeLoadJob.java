package com.yourteam.ingestion;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.yourteam.ingestion.config.DorisConfig;
import com.yourteam.ingestion.metrics.StreamingMetricsListener;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load generator: continuously produces ~{@value #ROWS_PER_SECOND} rows/sec of fake records (the
 * fixed 7-column layout) from Spark's {@code rate} source and writes them straight into Doris
 * {@code fake_kafka_test} — no Kafka involved. Used to load-test the Doris sink.
 *
 * <p>Everything is hardcoded below (edit the constants for your cluster). Run it from the same fat
 * jar as the main job, just with a different class/name:
 *
 * <pre>
 *   spark-submit --class com.yourteam.ingestion.FakeLoadJob --name spark-doris-fake-load \
 *     --conf "spark.driver.extraJavaOptions=$ADD_OPENS" \
 *     --conf "spark.executor.extraJavaOptions=$ADD_OPENS" \
 *     target/spark-doris-ingestion.jar
 * </pre>
 */
public final class FakeLoadJob {

    private static final Logger log = LoggerFactory.getLogger(FakeLoadJob.class);

    // ---- Hardcoded settings — edit for your environment ----
    private static final String APP_NAME = "spark-doris-fake-load";
    private static final String FENODES = "doris-fe:8030";
    private static final String DATABASE = "ods";
    private static final String TABLE = "fake_kafka_test";
    private static final String USER = "root";
    private static final String PASSWORD = "";                       // empty if the user has no password
    private static final long ROWS_PER_SECOND = 100_000L;            // ~100k QPS
    private static final String CHECKPOINT = "/tmp/ckpt/fake-load";  // use a durable path on a cluster

    private FakeLoadJob() {
    }

    /**
     * Maps a {@code rate}-source DataFrame (columns {@code timestamp}, {@code value}) to the fixed
     * 7 Doris columns, with sensible random fake content. Pure, so it is unit-testable.
     */
    public static Dataset<Row> fakeColumns(Dataset<Row> rate) {
        return rate.selectExpr(
                "timestamp as kafka_timestamp",
                "cast(pmod(value, 10) as int) as kafka_partition",
                "value as kafka_offset",
                "concat('order-', cast(value % 100000 as string)) as kafka_key",
                "to_json(named_struct("
                        + "'id', value, "
                        + "'amount', cast(rand() * 1000 as int), "
                        + "'item', element_at(array('coffee','tea','latte','mocha'), cast(rand() * 4 as int) + 1)"
                        + ")) as kafka_value",
                "to_json(array(named_struct('key', 'trace-id', 'value', uuid()))) as kafka_headers",
                "current_timestamp() as ingestion_time");
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName(APP_NAME).getOrCreate();
        spark.streams().addListener(new StreamingMetricsListener());

        Dataset<Row> rate = spark.readStream().format("rate")
                .option("rowsPerSecond", String.valueOf(ROWS_PER_SECOND))
                .load();
        Dataset<Row> fake = fakeColumns(rate);

        DorisConfig doris = DorisConfig.builder()
                .fenodes(FENODES).database(DATABASE).table(TABLE).user(USER).passwordEnv("")
                .options(Map.of("doris.sink.batch.size", "100000"))
                .build();

        DataStreamWriter<Row> writer = fake.writeStream()
                .format("doris")
                .outputMode("append")
                .option("checkpointLocation", CHECKPOINT);
        for (Map.Entry<String, String> e : IngestionPipeline.dorisOptions(doris, PASSWORD).entrySet()) {
            writer = writer.option(e.getKey(), e.getValue());
        }

        log.info("Fake load starting: ~{} rows/s -> Doris {}", ROWS_PER_SECOND, doris.tableIdentifier());
        StreamingQuery query;
        try {
            query = writer.start();
        } catch (TimeoutException e) {
            throw new RuntimeException("Failed to start fake-load query", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping fake-load query");
            try {
                query.stop();
            } catch (Exception e) {
                log.warn("Error while stopping query", e);
            }
        }, "fakeload-shutdown"));

        try {
            query.awaitTermination();
        } catch (Exception e) {
            log.error("Fake load failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}
