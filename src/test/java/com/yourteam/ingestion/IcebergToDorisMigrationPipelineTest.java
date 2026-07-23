package com.yourteam.ingestion;

import java.util.LinkedHashMap;
import java.util.Map;

import com.yourteam.ingestion.config.IcebergConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Pure assertions for the Iceberg catalog conf builder (no Spark needed). */
class IcebergToDorisMigrationPipelineTest {

    @Test
    void hiveCatalogConfIncludesTypeAndUri() {
        IcebergConfig iceberg = IcebergConfig.builder()
                .catalogName("iceberg")
                .catalogType("hive")
                .uri("thrift://metastore:9083")
                .database("lake").table("orders")
                .build();

        Map<String, String> conf = IcebergToDorisMigrationPipeline.icebergCatalogConf(iceberg);

        assertEquals(IcebergToDorisMigrationPipeline.ICEBERG_EXTENSIONS, conf.get("spark.sql.extensions"));
        assertEquals(IcebergToDorisMigrationPipeline.ICEBERG_CATALOG_IMPL, conf.get("spark.sql.catalog.iceberg"));
        assertEquals("hive", conf.get("spark.sql.catalog.iceberg.type"));
        assertEquals("thrift://metastore:9083", conf.get("spark.sql.catalog.iceberg.uri"));
        // No warehouse configured -> the key is absent, not blank.
        assertFalse(conf.containsKey("spark.sql.catalog.iceberg.warehouse"));
    }

    @Test
    void catalogTypeDefaultsToHiveWhenBlank() {
        IcebergConfig iceberg = IcebergConfig.builder()
                .catalogName("c").uri("thrift://m:9083").database("db").table("t").build();

        Map<String, String> conf = IcebergToDorisMigrationPipeline.icebergCatalogConf(iceberg);
        assertEquals("hive", conf.get("spark.sql.catalog.c.type"));
    }

    @Test
    void hadoopCatalogConfIncludesWarehouseAndPassThroughProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", "http://minio:9000");
        IcebergConfig iceberg = IcebergConfig.builder()
                .catalogName("lakehouse")
                .catalogType("hadoop")
                .warehouse("s3a://warehouse/")
                .database("db").table("t")
                .properties(props)
                .build();

        Map<String, String> conf = IcebergToDorisMigrationPipeline.icebergCatalogConf(iceberg);

        assertEquals("hadoop", conf.get("spark.sql.catalog.lakehouse.type"));
        assertEquals("s3a://warehouse/", conf.get("spark.sql.catalog.lakehouse.warehouse"));
        // Pass-through properties are prefixed with the catalog name.
        assertEquals("org.apache.iceberg.aws.s3.S3FileIO", conf.get("spark.sql.catalog.lakehouse.io-impl"));
        assertEquals("http://minio:9000", conf.get("spark.sql.catalog.lakehouse.s3.endpoint"));
    }
}
