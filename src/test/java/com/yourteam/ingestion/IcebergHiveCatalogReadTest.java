package com.yourteam.ingestion;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
 * Exercises the {@code catalog_type: hive} path — the one real deployments use — in-JVM, with no
 * Docker and no thrift server. The Hive Metastore client runs <em>embedded</em> against a local
 * Derby database (both supplied by the provided-scope {@code spark-hive}), covering what the
 * HadoopCatalog test cannot: that {@code HiveCatalog} resolves and reads through the production
 * wiring, and so that the Hive Metastore client really is on the classpath.
 *
 * <p>An embedded metastore is reached by leaving {@code hive.metastore.uris} empty, so this test
 * builds its {@link IcebergConfig} without a {@code uri} (via the builder, which does not run
 * validation). In production {@code iceberg.uri} is required — that rule is asserted separately in
 * {@code MigrationConfigLoaderTest}. Connecting over thrift is therefore the one part of the hive
 * path still exercised only against a real metastore.
 */
class IcebergHiveCatalogReadTest {

    private static final String CATALOG = "hivetest";
    private static SparkSession spark;
    private static IcebergConfig iceberg;

    @BeforeAll
    static void startSpark(@TempDir Path tmp) {
        // No uri, so the client resolves the ambient hive.metastore.uris — empty here, which is how
        // an embedded metastore is selected. Production configs must set iceberg.uri (validated).
        iceberg = IcebergConfig.builder()
                .catalogName(CATALOG)
                .catalogType("hive")
                .warehouse(tmp.resolve("warehouse").toString())
                .database("migdb").table("orders")
                .build();

        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("spark.master", "local[2]");
        conf.put("spark.ui.enabled", "false");
        conf.put("spark.sql.shuffle.partitions", "1");
        // Embedded metastore: no thrift URI, Derby-backed, all state under the temp dir.
        conf.put("spark.hadoop.hive.metastore.uris", "");
        conf.put("spark.hadoop.javax.jdo.option.ConnectionURL",
                "jdbc:derby:memory:" + System.nanoTime() + ";create=true");
        conf.put("spark.hadoop.hive.metastore.warehouse.dir", tmp.resolve("warehouse").toString());
        conf.put("spark.sql.warehouse.dir", tmp.resolve("warehouse").toString());
        conf.put("spark.sql.catalogImplementation", "hive");
        conf.put("derby.stream.error.file", tmp.resolve("derby.log").toString());
        // A fresh in-memory Derby has no metastore schema; let DataNucleus create it and skip the
        // version row check (a real deployment's metastore is already schema-initialised).
        conf.put("spark.hadoop.datanucleus.schema.autoCreateAll", "true");
        conf.put("spark.hadoop.hive.metastore.schema.verification", "false");
        // Iceberg commits take an HMS table lock by default, which needs the metastore's TXN tables
        // (HIVE_LOCKS/NEXT_LOCK_ID) — those are not part of the auto-created schema. Commit via the
        // metastore's own compare-and-swap instead; this only affects writes in this test's setup,
        // not the read path under test.
        conf.put("spark.hadoop.iceberg.engine.hive.lock-enabled", "false");

        MigrationConfig config = MigrationConfig.builder()
                .iceberg(iceberg)
                .doris(DorisConfig.builder().fenodes("fe:8030").database("ods").table("orders")
                        .user("u").build())
                .spark(SparkBatchConfig.builder().extraConf(conf).build())
                .build();

        spark = IcebergToDorisMigrationPipeline.buildSession(config, "IcebergHiveCatalogReadTest");
        // A SparkSession may already be active from an earlier test class; re-apply the runtime-
        // settable catalog keys so this catalog resolves regardless (see IcebergMigrationReadTest).
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
    void readsIcebergTableThroughHiveCatalog() {
        spark.sql("CREATE DATABASE IF NOT EXISTS " + CATALOG + "." + iceberg.getDatabase());
        spark.sql("CREATE TABLE " + iceberg.fullTableName()
                + " (id BIGINT, item STRING, amount INT) USING iceberg");
        spark.sql("INSERT INTO " + iceberg.fullTableName()
                + " VALUES (1, 'coffee', 300), (2, 'tea', 150)");

        Dataset<Row> df = IcebergToDorisMigrationPipeline.readIcebergTable(spark, iceberg);

        assertEquals(Arrays.asList("id", "item", "amount"), Arrays.asList(df.schema().fieldNames()));
        List<Row> rows = df.orderBy("id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals("coffee", rows.get(0).getAs("item"));
        assertEquals(Integer.valueOf(150), rows.get(1).<Integer>getAs("amount"));
    }
}
