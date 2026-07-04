/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.core.util.IgnoresEverything;
import net.openhft.chronicle.wire.metrics.CounterInstrument;
import net.openhft.chronicle.wire.metrics.LatencyInstrument;
import net.openhft.chronicle.wire.metrics.Metrics;

/**
 * Chronicle Queue's own metric touch points: the source names, metric names and per-queue
 * instruments the queue records into when a {@link net.openhft.chronicle.wire.metrics.MetricsBinding}
 * is installed and the source is enabled.
 * <p>
 * Instruments are <b>per queue</b>: each carries a {@code queue=<name>} label, where the name
 * is the queue's directory basename. They are registered in the process-global
 * {@code Metrics.registry(QueueMetrics.APPENDER_SOURCE)} at queue/appender construction;
 * registration dedups on (kind, name, labels), so two appenders on the same queue share the
 * same thread-safe instruments. Flushing them to the installed binding is the host's job - e.g.
 * {@code Metrics.registry(QueueMetrics.APPENDER_SOURCE).flush(out, eventTime, intervalNs)}
 * on its monitor thread.
 * <p>
 * <b>Startup-only (resolve-once) policy.</b> These are hot-path touch points, so they use
 * {@link Metrics#forSourceStatic(String)} semantics: {@link #appenderEnabled()} is evaluated
 * once at queue/appender construction and cached in a {@code final boolean}. An appender (or
 * roll listener) constructed <em>before</em> the binding is installed records nothing, even
 * after a later {@link Metrics#install install()} - install the binding before constructing
 * the components you want measured. When nothing is installed at construction time the touch
 * points cost one cached-boolean check.
 * <p>
 * <b>Latency coverage.</b> {@value #WRITE_LATENCY_NS} is <em>appends-only latency</em>: it is
 * sampled only for excerpts opened through {@code writingDocument()} (which includes
 * {@code writeBytes(WriteBytesMarshallable)} and method writers). The raw
 * {@code writeBytes(BytesStore)} / {@code writeBytes(index, BytesStore)} paths increment
 * {@value #APPENDS_TOTAL} but do not sample latency. Metadata writes count neither.
 */
public final class QueueMetrics {

    /**
     * The dotted source the appender-side touch points are registered under.
     */
    public static final String APPENDER_SOURCE = "chronicle.queue.appender";

    /**
     * The dotted source of the metrics machinery's own self-metrics, e.g. the
     * {@link QueueMetricsBinding}'s dropped-events counter.
     */
    public static final String METRICS_SOURCE = "chronicle.queue.metrics";

    /**
     * Counter of committed excerpts appended, per queue ({@code queue=<name>} label).
     */
    public static final String APPENDS_TOTAL = "chronicle_queue_appends_total";

    /**
     * Histogram of excerpt write latency (writingDocument open to commit), in nanoseconds,
     * per queue ({@code queue=<name>} label). Appends-only latency: raw
     * {@code writeBytes(BytesStore)} paths are not sampled - see the class javadoc.
     */
    public static final String WRITE_LATENCY_NS = "chronicle_queue_write_latency_ns";

    /**
     * Counter of store files acquired (cycle rolls and first opens), per queue
     * ({@code queue=<name>} label).
     */
    public static final String ROLLS_TOTAL = "chronicle_queue_rolls_total";

    /**
     * Counter of metric events dropped by a {@link QueueMetricsBinding} because a write
     * failed; emitted under {@value #METRICS_SOURCE} on the binding's next successful write.
     */
    public static final String DROPPED_TOTAL = "chronicle_queue_metrics_dropped_total";

    private QueueMetrics() {
    }

    /**
     * Returns whether the {@value #APPENDER_SOURCE} source currently resolves to a real
     * destination, i.e. a binding is installed and the source is enabled. This is the
     * resolve-once fast path ({@link Metrics#forSourceStatic(String)}): touch points call it
     * once at construction and cache the result in a {@code final boolean}, so a disabled
     * source costs nothing on the hot path - and a later install is deliberately not
     * observed by already-constructed components (see the class javadoc).
     *
     * @return {@code true} if appender metrics should be recorded by components constructed now
     */
    public static boolean appenderEnabled() {
        return !(Metrics.forSourceStatic(APPENDER_SOURCE) instanceof IgnoresEverything);
    }

    /**
     * Returns the appends counter for the given queue, registering it on first use. Dedup on
     * (kind, name, labels) means every appender of the same queue shares one instrument.
     *
     * @param queueName the queue's identity, normally its directory basename
     * @return the {@value #APPENDS_TOTAL} instrument labelled {@code queue=<queueName>}
     */
    public static CounterInstrument appends(String queueName) {
        return Metrics.registry(APPENDER_SOURCE).counter(APPENDS_TOTAL, queueLabel(queueName));
    }

    /**
     * Returns the write-latency histogram for the given queue, registering it on first use.
     *
     * @param queueName the queue's identity, normally its directory basename
     * @return the {@value #WRITE_LATENCY_NS} instrument labelled {@code queue=<queueName>}
     */
    public static LatencyInstrument writeLatency(String queueName) {
        return Metrics.registry(APPENDER_SOURCE).latency(WRITE_LATENCY_NS, queueLabel(queueName))
                .unit("ns");
    }

    /**
     * Returns the roll counter for the given queue, registering it on first use.
     *
     * @param queueName the queue's identity, normally its directory basename
     * @return the {@value #ROLLS_TOTAL} instrument labelled {@code queue=<queueName>}
     */
    public static CounterInstrument rolls(String queueName) {
        return Metrics.registry(APPENDER_SOURCE).counter(ROLLS_TOTAL, queueLabel(queueName));
    }

    /**
     * Builds the {@code queue=<name>} label string, sanitizing the value so any directory
     * basename is a valid label value ({@code '='} and {@code ';'} are structural in the
     * concatenated labels form and are replaced with {@code '_'}).
     *
     * @param queueName the queue's identity, normally its directory basename
     * @return the labels string {@code "queue=<sanitized-name>"}
     */
    static String queueLabel(String queueName) {
        String value = queueName == null || queueName.isEmpty() ? "unnamed"
                : queueName.replace('=', '_').replace(';', '_');
        return "queue=" + value;
    }
}
