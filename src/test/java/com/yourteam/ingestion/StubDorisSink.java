package com.yourteam.ingestion;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

/**
 * In-JVM stand-in for the Doris sink used by the integration tests: a {@code foreachBatch} writer
 * that captures every row that would have been written into {@link #WRITTEN}, so a test can assert
 * exactly what landed without a real Doris cluster. Checkpointing is real, so resume-from-offset
 * behaves as it would in production.
 *
 * <p>The literal {@code .format("doris")} write (in {@link IngestionPipeline#dorisWriter}) is the
 * only line not exercised here — it needs a live FE. The exact option contract handed to that
 * connector is asserted separately via {@link IngestionPipeline#dorisOptions}.
 */
final class StubDorisSink {

    /** Rows captured across all batches of the current run; reset between runs with {@link #reset}. */
    static final List<Row> WRITTEN = new CopyOnWriteArrayList<>();

    private StubDorisSink() {
    }

    static void reset() {
        WRITTEN.clear();
    }

    /** Starts a streaming write that appends each batch's rows to {@link #WRITTEN}. */
    static StreamingQuery start(Dataset<Row> doris, String checkpointLocation, Trigger trigger)
            throws TimeoutException {
        DataStreamWriter<Row> writer = doris.writeStream()
                .outputMode("append")
                .option("checkpointLocation", checkpointLocation)
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) ->
                        WRITTEN.addAll(batch.collectAsList()));
        return (trigger == null ? writer : writer.trigger(trigger)).start();
    }
}
