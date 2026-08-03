package com.yourteam.ingestion;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.yourteam.ingestion.config.DorisConfig;
import com.yourteam.ingestion.config.IcebergConfig;
import com.yourteam.ingestion.config.MigrationConfig;
import com.yourteam.ingestion.config.SparkBatchConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * In-JVM integration test for the migration read path. Uses a real Iceberg {@code HadoopCatalog}
 * over a temp warehouse (no Hive Metastore, no Docker) to exercise the production
 * {@link IcebergToDorisMigrationPipeline#buildSession} / {@link IcebergToDorisMigrationPipeline#readIcebergTable}:
 * a table is created and populated through Spark, then read back — asserting the source schema and
 * rows are preserved verbatim (the "keep the original schema" contract). The Doris write itself
 * ({@code .format("doris").save()}) needs a live Doris and is exercised only in a real environment;
 * its option/mode wiring is asserted separately.
 */
class IcebergMigrationReadTest {

    private static final String CATALOG = "migtest";
    private static SparkSession spark;
    private static IcebergConfig iceberg;

    @BeforeAll
    static void startSpark(@TempDir Path warehouse) {
        iceberg = IcebergConfig.builder()
                .catalogName(CATALOG)
                .catalogType("hadoop")
                .warehouse(warehouse.toString())
                .database("db").table("orders")
                .build();

        MigrationConfig config = MigrationConfig.builder()
                .iceberg(iceberg)
                .doris(DorisConfig.builder().fenodes("fe:8030").database("ods").table("orders")
                        .user("u").build())
                .spark(SparkBatchConfig.builder()
                        .extraConf(Map.of(
                                "spark.master", "local[2]",
                                "spark.ui.enabled", "false",
                                "spark.sql.shuffle.partitions", "1"))
                        .build())
                .build();

        spark = IcebergToDorisMigrationPipeline.buildSession(config, "IcebergMigrationReadTest");
        // Defensive: if a prior test left a shared SparkSession active, getOrCreate returned it and
        // the build-time catalog conf was ignored. Iceberg reads the catalog from the runtime conf
        // lazily on first access, so re-setting the catalog keys here guarantees it resolves in-suite.
        // (spark.sql.extensions is a static build-time conf and cannot be set at runtime — it is not
        // needed for a plain read, so skip it.)
        for (Map.Entry<String, String> e : IcebergToDorisMigrationPipeline.icebergCatalogConf(iceberg).entrySet()) {
            if (e.getKey().startsWith("spark.sql.catalog.")) {
                spark.conf().set(e.getKey(), e.getValue());
            }
        }
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void readsIcebergTablePreservingSchemaAndRows() {
        spark.sql("CREATE TABLE " + iceberg.fullTableName()
                + " (id BIGINT, item STRING, amount INT) USING iceberg");
        spark.sql("INSERT INTO " + iceberg.fullTableName()
                + " VALUES (1, 'coffee', 300), (2, 'tea', 150), (3, 'latte', 420)");

        Dataset<Row> df = IcebergToDorisMigrationPipeline.readIcebergTable(spark, iceberg);

        // Schema is preserved verbatim (names + order), not remapped to the Kafka 7-column layout.
        assertEquals(Arrays.asList("id", "item", "amount"), Arrays.asList(df.schema().fieldNames()));

        List<Row> rows = df.orderBy("id").collectAsList();
        assertEquals(3, rows.size());
        assertEquals(Long.valueOf(1L), rows.get(0).<Long>getAs("id"));
        assertEquals("coffee", rows.get(0).getAs("item"));
        assertEquals(Integer.valueOf(300), rows.get(0).<Integer>getAs("amount"));
        assertEquals("latte", rows.get(2).getAs("item"));
    }

    @Test
    void dorisWriteOptionsCarryConnectionForOverwrite() {
        // The write path hands these options to the Doris connector (mode = Overwrite, full replace).
        DorisConfig doris = DorisConfig.builder().fenodes("fe:8030").database("ods")
                .table("orders").user("ingest").passwordEnv("DORIS_PASSWORD").build();
        Map<String, String> opts = IngestionPipeline.dorisOptions(doris, "s3cret");
        assertEquals("ods.orders", opts.get("doris.table.identifier"));
        assertEquals("fe:8030", opts.get("doris.fenodes"));
        assertEquals("ingest", opts.get("doris.user"));
        assertEquals("s3cret", opts.get("doris.password"));
    }
}
