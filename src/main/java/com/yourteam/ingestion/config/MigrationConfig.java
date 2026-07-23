package com.yourteam.ingestion.config;

import java.util.ArrayList;
import java.util.List;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Root config for the one-shot Iceberg -&gt; Doris migration job. Parsed from YAML by
 * {@link ConfigLoader}, which calls {@link #validate()} before returning it.
 *
 * <p>Reuses {@link DorisConfig} for the sink (including its {@code password_env} handling); the
 * source is an Iceberg table ({@link IcebergConfig}) instead of Kafka, and the Spark section is
 * the batch-only {@link SparkBatchConfig} (no checkpoint/trigger).
 */
@Value
@Builder
@Jacksonized
public class MigrationConfig implements Validatable {

    IcebergConfig iceberg;
    DorisConfig doris;

    /** Optional. Extra Spark conf; defaults to empty when the {@code spark:} section is omitted. */
    @Builder.Default
    SparkBatchConfig spark = SparkBatchConfig.builder().build();

    @Override
    public void validate() {
        List<String> missing = new ArrayList<>();
        if (iceberg == null) {
            missing.add("iceberg");
        } else {
            iceberg.collectMissing(missing);
        }
        if (doris == null) {
            missing.add("doris");
        } else {
            doris.collectMissing(missing);
        }
        // spark is optional (defaults to empty extra_conf).
        if (!missing.isEmpty()) {
            throw new ConfigException(
                    "Invalid config — missing/blank required fields: " + String.join(", ", missing));
        }
    }
}
