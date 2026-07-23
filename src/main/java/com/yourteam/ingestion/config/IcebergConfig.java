package com.yourteam.ingestion.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Iceberg source settings for the one-shot migration job. These drive the Spark v2 catalog
 * registration ({@code spark.sql.catalog.<name>.*}) and name the table to migrate.
 *
 * <p>The default catalog type is {@code hive} (read through a Hive Metastore); {@code hadoop} and
 * {@code rest} are also accepted. {@link #uri} is the Hive Metastore thrift URI (required for a
 * hive catalog); {@link #warehouse} is the storage root (required for a hadoop catalog, optional
 * otherwise). Anything else the catalog needs goes in {@link #properties} verbatim.
 */
@Value
@Builder
@Jacksonized
public class IcebergConfig {

    /** Required. Spark catalog name to register the Iceberg catalog under, e.g. {@code iceberg}. */
    String catalogName;

    /** Optional. Catalog type: {@code hive} (default), {@code hadoop}, or {@code rest}. */
    @Builder.Default
    String catalogType = "hive";

    /** Hive Metastore thrift URI, e.g. {@code thrift://metastore:9083}. Required for a hive catalog. */
    String uri;

    /** Storage warehouse root. Required for a hadoop catalog; optional (an override) otherwise. */
    String warehouse;

    /** Required. Source database (Iceberg namespace). */
    String database;

    /** Required. Source table. */
    String table;

    /**
     * Optional. Extra Iceberg catalog properties passed through verbatim. Each key is the suffix
     * after {@code spark.sql.catalog.<name>.} — e.g. {@code io-impl} or {@code s3.endpoint}.
     */
    @Builder.Default
    Map<String, String> properties = Collections.emptyMap();

    /** The catalog type, defaulting to {@code hive} when blank. */
    public String resolvedCatalogType() {
        return Configs.isBlank(catalogType) ? "hive" : catalogType.trim();
    }

    /** Fully-qualified table name for the v2 catalog: {@code catalog.database.table}. */
    public String fullTableName() {
        return catalogName + "." + database + "." + table;
    }

    void collectMissing(List<String> missing) {
        if (Configs.isBlank(catalogName)) {
            missing.add("iceberg.catalog_name");
        }
        if (Configs.isBlank(database)) {
            missing.add("iceberg.database");
        }
        if (Configs.isBlank(table)) {
            missing.add("iceberg.table");
        }
        String type = resolvedCatalogType();
        if ("hive".equalsIgnoreCase(type) && Configs.isBlank(uri)) {
            missing.add("iceberg.uri (required for a hive catalog)");
        }
        if ("hadoop".equalsIgnoreCase(type) && Configs.isBlank(warehouse)) {
            missing.add("iceberg.warehouse (required for a hadoop catalog)");
        }
    }
}
