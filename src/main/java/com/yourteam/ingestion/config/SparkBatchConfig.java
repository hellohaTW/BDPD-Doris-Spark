package com.yourteam.ingestion.config;

import java.util.Collections;
import java.util.Map;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Spark settings for the one-shot (batch) migration job. Unlike the streaming
 * {@link SparkStreamingConfig} there is no checkpoint/trigger/output-mode — a batch write runs
 * once — so this holds only arbitrary {@code spark.*} conf applied to the SparkSession (e.g.
 * {@code spark.master} for an off-cluster local run, or {@code spark.sql.shuffle.partitions}).
 */
@Value
@Builder
@Jacksonized
public class SparkBatchConfig {

    /** Optional. Extra SparkSession conf, applied via {@code .config(k, v)}. */
    @Builder.Default
    Map<String, String> extraConf = Collections.emptyMap();
}
