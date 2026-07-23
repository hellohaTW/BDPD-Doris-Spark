package com.yourteam.ingestion.transform;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.to_json;

/**
 * Maps a Kafka source DataFrame to the fixed Doris target layout.
 *
 * <p>The job is schema-agnostic: the message payload is never parsed. {@code kafka_value}
 * (and {@code kafka_key}) are carried verbatim as strings. Every Doris table shares the
 * same seven columns, in DDL order:
 *
 * <pre>
 * kafka_timestamp, kafka_partition, kafka_offset, kafka_key, kafka_value, kafka_headers, ingestion_time
 * </pre>
 *
 * <p>The Kafka reader must set {@code includeHeaders=true} so the {@code headers} column exists.
 */
public final class MessageTransform {

    private MessageTransform() {
    }

    /**
     * Projects the standard Spark Kafka source schema onto the fixed Doris columns.
     *
     * @param kafka a DataFrame from {@code spark.readStream().format("kafka")} with
     *              {@code includeHeaders=true}
     * @return a DataFrame with exactly the seven Doris columns
     */
    public static Dataset<Row> toDorisColumns(Dataset<Row> kafka) {
        // Render headers (array<struct<key:string,value:binary>>) as clean JSON by casting
        // each value to string first; otherwise to_json emits base64-encoded bytes.
        Column headersAsJson = to_json(
                expr("transform(headers, h -> struct(h.key AS key, CAST(h.value AS STRING) AS value))"));

        return kafka.select(
                col("timestamp").alias("kafka_timestamp"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("kafka_value"),
                headersAsJson.alias("kafka_headers"),
                current_timestamp().alias("ingestion_time"));
    }
}
