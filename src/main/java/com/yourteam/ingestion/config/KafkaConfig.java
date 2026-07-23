package com.yourteam.ingestion.config;

import java.util.List;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/**
 * Kafka source settings. YAML keys are snake_case (e.g. {@code bootstrap_servers}); the
 * loader's ObjectMapper maps them to these camelCase fields.
 */
@Value
@Builder
@Jacksonized
public class KafkaConfig {

    /** Required. {@code kafka.bootstrap.servers} for the Spark Kafka source. */
    String bootstrapServers;

    /** Required. Topic to subscribe to. */
    String topic;

    /** Optional. {@code startingOffsets} — defaults to {@code latest}. */
    @Builder.Default
    String startingOffsets = "latest";

    /** Optional. {@code maxOffsetsPerTrigger} rate limit; null means unbounded. */
    Long maxOffsetsPerTrigger;

    void collectMissing(List<String> missing) {
        if (Configs.isBlank(bootstrapServers)) {
            missing.add("kafka.bootstrap_servers");
        }
        if (Configs.isBlank(topic)) {
            missing.add("kafka.topic");
        }
    }
}
