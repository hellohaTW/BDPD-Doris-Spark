package com.yourteam.ingestion.metrics;

import java.util.function.Consumer;

import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs structured (JSON) streaming metrics by listening to the query lifecycle (Task 5): one
 * {@code query_started} event, a {@code batch_progress} event per micro-batch (input/processed
 * rows per second, batch id, row counts), and a {@code query_terminated} event.
 *
 * <p>Thin adapter — all formatting lives in {@link StreamingMetrics} (unit-tested); this class
 * only pulls fields off the Spark events and hands the JSON to a sink. The sink defaults to the
 * logger; tests inject a capturing one so the adapter is exercised on a real Spark query without
 * depending on which SLF4J binding is active. Register it with
 * {@code spark.streams().addListener(new StreamingMetricsListener())}.
 */
public class StreamingMetricsListener extends StreamingQueryListener {

    private static final Logger log = LoggerFactory.getLogger(StreamingMetricsListener.class);

    private final Consumer<String> infoSink;
    private final Consumer<String> warnSink;

    /** Production: events go to the logger as JSON lines. */
    public StreamingMetricsListener() {
        this(log::info, log::warn);
    }

    /** Test seam: route emitted JSON to the given sinks instead of the logger. */
    StreamingMetricsListener(Consumer<String> infoSink, Consumer<String> warnSink) {
        this.infoSink = infoSink;
        this.warnSink = warnSink;
    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {
        infoSink.accept(StreamingMetrics.queryStarted(
                String.valueOf(event.id()), String.valueOf(event.runId()), event.name()));
    }

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        StreamingQueryProgress p = event.progress();
        infoSink.accept(StreamingMetrics.batchProgress(
                p.name(), p.batchId(), p.timestamp(), p.numInputRows(),
                p.inputRowsPerSecond(), p.processedRowsPerSecond()));
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
        String exception = event.exception().isDefined() ? event.exception().get() : null;
        String json = StreamingMetrics.queryTerminated(
                String.valueOf(event.id()), String.valueOf(event.runId()), exception);
        (exception != null ? warnSink : infoSink).accept(json);
    }
}
