package com.yourteam.ingestion.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Spark Structured Streaming settings for the job (checkpoint, trigger, output mode) plus
 * arbitrary {@code spark.*} conf applied to the SparkSession.
 */
@Value
@Builder
@Jacksonized
public class SparkStreamingConfig {

    /** Required. Streaming checkpoint location. */
    String checkpointLocation;

    /** Optional. Streaming output mode — defaults to {@code append}. */
    @Builder.Default
    String outputMode = "append";

    /**
     * Optional. Trigger spec, e.g. {@code "30 seconds"}, {@code "once"}, {@code "availableNow"}.
     * Null means Spark's default (micro-batch as fast as possible).
     */
    String trigger;

    /** Optional. Extra SparkSession conf, applied as {@code spark.conf().set(k, v)}. */
    @Builder.Default
    Map<String, String> extraConf = Collections.emptyMap();

    void collectMissing(List<String> missing) {
        if (Configs.isBlank(checkpointLocation)) {
            missing.add("spark.checkpoint_location");
        }
    }
}
