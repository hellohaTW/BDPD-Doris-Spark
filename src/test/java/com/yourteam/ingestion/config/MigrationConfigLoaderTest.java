package com.yourteam.ingestion.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Loading + fail-fast validation for the one-shot migration config ({@link MigrationConfig}). */
class MigrationConfigLoaderTest {

    private static InputStream yaml(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void loadsAllFieldsFromValidYaml() {
        MigrationConfig cfg = ConfigLoader.load(yaml(
                "iceberg:\n"
                        + "  catalog_name: \"iceberg\"\n"
                        + "  catalog_type: \"hive\"\n"
                        + "  uri: \"thrift://metastore:9083\"\n"
                        + "  database: \"lake\"\n"
                        + "  table: \"orders\"\n"
                        + "  properties:\n"
                        + "    io-impl: \"org.apache.iceberg.aws.s3.S3FileIO\"\n"
                        + "doris:\n"
                        + "  fenodes: \"fe:8030\"\n"
                        + "  database: \"ods\"\n"
                        + "  table: \"orders\"\n"
                        + "  user: \"ingest\"\n"
                        + "  password_env: \"DORIS_PASSWORD\"\n"
                        + "  options:\n"
                        + "    doris.sink.batch.size: \"100000\"\n"
                        + "spark:\n"
                        + "  extra_conf:\n"
                        + "    spark.sql.shuffle.partitions: \"16\"\n"),
                MigrationConfig.class);

        assertEquals("iceberg", cfg.getIceberg().getCatalogName());
        assertEquals("hive", cfg.getIceberg().resolvedCatalogType());
        assertEquals("thrift://metastore:9083", cfg.getIceberg().getUri());
        assertEquals("iceberg.lake.orders", cfg.getIceberg().fullTableName());
        assertEquals("org.apache.iceberg.aws.s3.S3FileIO",
                cfg.getIceberg().getProperties().get("io-impl"));

        assertEquals("ods.orders", cfg.getDoris().tableIdentifier());
        assertEquals("100000", cfg.getDoris().getOptions().get("doris.sink.batch.size"));

        assertEquals("16", cfg.getSpark().getExtraConf().get("spark.sql.shuffle.partitions"));
    }

    @Test
    void appliesDefaultsWhenOptionalFieldsOmitted() {
        MigrationConfig cfg = ConfigLoader.load(yaml(
                "iceberg:\n"
                        + "  catalog_name: \"iceberg\"\n"
                        + "  uri: \"thrift://m:9083\"\n"
                        + "  database: \"lake\"\n"
                        + "  table: \"orders\"\n"
                        + "doris:\n"
                        + "  fenodes: \"fe:8030\"\n"
                        + "  database: \"ods\"\n"
                        + "  table: \"orders\"\n"
                        + "  user: \"u\"\n"),
                MigrationConfig.class);

        // catalog_type defaults to hive; omitted maps are empty, never null.
        assertEquals("hive", cfg.getIceberg().resolvedCatalogType());
        assertEquals(Collections.emptyMap(), cfg.getIceberg().getProperties());
        assertEquals(Collections.emptyMap(), cfg.getDoris().getOptions());
        // spark section omitted entirely -> empty extra_conf.
        assertEquals(Collections.emptyMap(), cfg.getSpark().getExtraConf());
    }

    @Test
    void failsFastListingEveryMissingRequiredField() {
        // Missing iceberg.table and doris.fenodes; hive catalog with no uri.
        String bad =
                "iceberg:\n"
                        + "  catalog_name: \"iceberg\"\n"
                        + "  database: \"lake\"\n"
                        + "doris:\n"
                        + "  database: \"ods\"\n"
                        + "  table: \"orders\"\n"
                        + "  user: \"u\"\n";

        ConfigException ex = assertThrows(ConfigException.class,
                () -> ConfigLoader.load(yaml(bad), MigrationConfig.class));
        String msg = ex.getMessage();
        assertTrue(msg.contains("iceberg.table"), msg);
        assertTrue(msg.contains("iceberg.uri"), msg);
        assertTrue(msg.contains("doris.fenodes"), msg);
    }

    @Test
    void hadoopCatalogRequiresWarehouseNotUri() {
        // A hadoop catalog needs a warehouse; uri is not required for it.
        String bad =
                "iceberg:\n"
                        + "  catalog_name: \"iceberg\"\n"
                        + "  catalog_type: \"hadoop\"\n"
                        + "  database: \"lake\"\n"
                        + "  table: \"orders\"\n"
                        + "doris: {fenodes: fe, database: ods, table: t, user: u}\n";

        ConfigException ex = assertThrows(ConfigException.class,
                () -> ConfigLoader.load(yaml(bad), MigrationConfig.class));
        assertTrue(ex.getMessage().contains("iceberg.warehouse"), ex.getMessage());
    }
}
