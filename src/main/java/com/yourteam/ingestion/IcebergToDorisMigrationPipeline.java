package com.yourteam.ingestion;

import java.util.LinkedHashMap;
import java.util.Map;

import com.yourteam.ingestion.config.DorisConfig;
import com.yourteam.ingestion.config.IcebergConfig;
import com.yourteam.ingestion.config.MigrationConfig;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * The pieces of the one-shot Iceberg -&gt; Doris migration, factored out of
 * {@link IcebergMigrationJob#main} so each step is unit-testable. The Iceberg catalog conf and the
 * Doris options are pure and asserted directly; the read path is exercised in-JVM against a
 * HadoopCatalog; the literal {@code .format("doris").save()} write is the only line that needs a
 * live Doris (mirrors how the streaming job treats its Doris write).
 */
public final class IcebergToDorisMigrationPipeline {

    /** The Spark v2 catalog implementation Iceberg registers under {@code spark.sql.catalog.<name>}. */
    static final String ICEBERG_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog";

    /** Iceberg's SQL extensions, enabled so the catalog behaves like a full Iceberg catalog. */
    static final String ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";

    private IcebergToDorisMigrationPipeline() {
    }

    /**
     * The SparkSession conf that registers the Iceberg source catalog: the catalog impl/type, its
     * URI/warehouse when set, any pass-through {@code properties}, and Iceberg's SQL extensions.
     * Pure and insertion-ordered so it can be asserted directly.
     */
    public static Map<String, String> icebergCatalogConf(IcebergConfig iceberg) {
        String name = iceberg.getCatalogName();
        String prefix = "spark.sql.catalog." + name;
        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("spark.sql.extensions", ICEBERG_EXTENSIONS);
        conf.put(prefix, ICEBERG_CATALOG_IMPL);
        conf.put(prefix + ".type", iceberg.resolvedCatalogType());
        if (iceberg.getUri() != null && !iceberg.getUri().trim().isEmpty()) {
            conf.put(prefix + ".uri", iceberg.getUri());
        }
        if (iceberg.getWarehouse() != null && !iceberg.getWarehouse().trim().isEmpty()) {
            conf.put(prefix + ".warehouse", iceberg.getWarehouse());
        }
        for (Map.Entry<String, String> e : iceberg.getProperties().entrySet()) {
            conf.put(prefix + "." + e.getKey(), e.getValue());
        }
        return conf;
    }

    /**
     * Builds the SparkSession, applying the Iceberg catalog conf first and then the user's extra
     * {@code spark.*} conf (which may override it). Master comes from conf/spark-submit.
     */
    public static SparkSession buildSession(MigrationConfig config, String appName) {
        SparkSession.Builder builder = SparkSession.builder().appName(appName);
        for (Map.Entry<String, String> e : icebergCatalogConf(config.getIceberg()).entrySet()) {
            builder = builder.config(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, String> e : config.getSpark().getExtraConf().entrySet()) {
            builder = builder.config(e.getKey(), e.getValue());
        }
        return builder.getOrCreate();
    }

    /** Reads the Iceberg source table as a DataFrame, preserving its schema verbatim. */
    public static Dataset<Row> readIcebergTable(SparkSession spark, IcebergConfig iceberg) {
        return spark.table(iceberg.fullTableName());
    }

    /**
     * Writes the DataFrame into Doris with {@link SaveMode#Overwrite} (the connector replaces the
     * whole target table). The Doris connection/options come from {@link IngestionPipeline#dorisOptions}.
     * The target table must already exist with a compatible schema — this job never issues DDL.
     */
    public static void writeToDoris(Dataset<Row> df, DorisConfig doris, String password) {
        DataFrameWriter<Row> writer = df.write().format("doris").mode(SaveMode.Overwrite);
        for (Map.Entry<String, String> e : IngestionPipeline.dorisOptions(doris, password).entrySet()) {
            writer = writer.option(e.getKey(), e.getValue());
        }
        writer.save();
    }
}
