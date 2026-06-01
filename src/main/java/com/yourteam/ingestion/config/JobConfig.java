package com.yourteam.ingestion.config;

import java.util.ArrayList;
import java.util.List;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Root job config: the three sections that drive one ingestion job. Parsed from YAML by
 * {@link ConfigLoader}, which calls {@link #validate()} before returning it.
 */
@Value
@Builder
@Jacksonized
public class JobConfig {

    KafkaConfig kafka;
    DorisConfig doris;
    SparkStreamingConfig spark;

    /** Optional. Restart/retry policy; defaults applied when the {@code retry:} section is omitted. */
    @Builder.Default
    RetryConfig retry = RetryConfig.defaults();

    /**
     * Fails fast if any required section or field is missing/blank. Collects every problem
     * into one message rather than failing on the first.
     *
     * @throws ConfigException listing all missing/blank required fields
     */
    public void validate() {
        List<String> missing = new ArrayList<>();
        if (kafka == null) {
            missing.add("kafka");
        } else {
            kafka.collectMissing(missing);
        }
        if (doris == null) {
            missing.add("doris");
        } else {
            doris.collectMissing(missing);
        }
        if (spark == null) {
            missing.add("spark");
        } else {
            spark.collectMissing(missing);
        }
        // retry is optional with defaults; only present (or default) instances are range-checked.
        if (retry != null) {
            retry.collectMissing(missing);
        }
        if (!missing.isEmpty()) {
            throw new ConfigException(
                    "Invalid config — missing/blank required fields: " + String.join(", ", missing));
        }
    }
}
