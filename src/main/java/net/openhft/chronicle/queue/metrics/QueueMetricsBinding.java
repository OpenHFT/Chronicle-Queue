/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.ServicesTimestampLongConverter;
import net.openhft.chronicle.wire.metrics.CounterMetric;
import net.openhft.chronicle.wire.metrics.GaugeMetric;
import net.openhft.chronicle.wire.metrics.HistogramMetric;
import net.openhft.chronicle.wire.metrics.Metrics;
import net.openhft.chronicle.wire.metrics.MetricsBinding;
import net.openhft.chronicle.wire.metrics.MetricsOut;
import net.openhft.chronicle.wire.metrics.PointEvent;
import net.openhft.chronicle.wire.metrics.RateMetric;
import net.openhft.chronicle.wire.metrics.SourceSelection;
import net.openhft.chronicle.wire.metrics.ThreadLocalisedMetricsOut;
import net.openhft.chronicle.core.util.IgnoresEverything;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * The production {@link MetricsBinding}: metric events are written to a dedicated Chronicle
 * Queue through a {@link MetricsOut} method writer, so the on-queue schema is exactly the
 * wire interface - {@code counterMetric: { ... }}, {@code histogramMetric: { ... }}, ... -
 * and any {@code MethodReader} (the metrics gateway, a same-host tailer, a test) reads them
 * back with the standard reused-DTO discipline.
 * <p>
 * On creation, if the queue is empty, a {@link MetricsFormat} header event ({@code
 * metricsFormat}, magic {@value MetricsFormat#MAGIC}, version {@value MetricsFormat#FORMAT_VERSION})
 * is written as the first excerpt so consumers can refuse an incompatible producer loudly.
 * <p>
 * <b>Failure policy</b> - never block, never throw into product code: a write failure (disk
 * full, queue closed during shutdown) is caught, counted in {@link #droppedCount()} and logged
 * once via {@link Jvm#warn()}; subsequent failures are counted silently. The dropped count is
 * also exposed as a real self-metric: on the next successful write after a drop, a
 * {@value QueueMetrics#DROPPED_TOTAL} counter event (source
 * {@value QueueMetrics#METRICS_SOURCE}) is emitted through the global {@code Metrics} facade -
 * so it appears whenever a binding is installed and that source is enabled, and observes late
 * installs per the cadence/event emission policy.
 * <p>
 * <b>Method writers.</b> Writers are not thread-safe, so each recording thread lazily gets its
 * own writer over the queue's thread-local appender; every source shares them ({@code source}
 * is carried on each event, so no per-source state is needed). The recording path expects the
 * <em>generated</em> (compile-time) method writer: Wire's {@link Proxy}-based fallback
 * allocates an {@code Object[]} per invocation. If only a proxy writer is available (e.g.
 * {@code -DdisableProxyCodegen=true} or codegen failure), construction <b>fails fast</b> with
 * {@link IllegalStateException} unless the {@value #ALLOW_PROXY_WRITER_PROPERTY} system
 * property is {@code true} (default {@code false}), in which case it warns and proceeds.
 * <p>
 * <b>Retention.</b> The metrics queue is an ordinary Chronicle Queue and grows on disk until
 * its cycle files are removed. The default roll cycle is deliberately short
 * ({@link RollCycles#FAST_HOURLY}) so files age out at a fine granularity; it is configurable
 * via {@link #at(File, SourceSelection, RollCycle)}. Apply the standard retention tooling to
 * the metrics directory - a {@code StoreFileListener} that archives/deletes released cycles,
 * or an external tidy-up job - sized to how far behind your metrics gateway may fall. See
 * {@code docs/queue-metrics.adoc}.
 * <p>
 * <b>Lifecycle.</b> {@link #install()} installs this binding process-wide and keeps the
 * {@link Metrics.Installation} handle; {@link #close()} uninstalls it again (restoring the
 * previously installed binding) before closing an owned queue, so the global {@code Metrics}
 * never routes to a closed binding.
 */
public final class QueueMetricsBinding implements MetricsBinding, Closeable {

    /**
     * System property ({@code true}/{@code false}, default {@code false}): permit falling
     * back to a {@link Proxy}-based (allocating) method writer instead of failing fast at
     * construction when the generated writer is unavailable.
     */
    public static final String ALLOW_PROXY_WRITER_PROPERTY = "chronicle.queue.metrics.allowProxyWriter";

    @NotNull
    private final ChronicleQueue queue;
    private final boolean ownsQueue;
    @Nullable
    private final SourceSelection selection;
    private final boolean allowProxyWriter = Jvm.getBoolean(ALLOW_PROXY_WRITER_PROPERTY);
    // Each thread owns one method writer; created lazily on first use per thread.
    private final ThreadLocal<MetricsOut> writers = ThreadLocal.withInitial(this::newWriter);
    private final DropNotThrowOut out = new DropNotThrowOut();
    private final AtomicBoolean warnedOnce = new AtomicBoolean();
    // Count of metric events dropped because a write failed; thread-safe, lock-free.
    private final LongAdder dropped = new LongAdder();
    private static final long REPORTING_DROPPED = Long.MIN_VALUE;

    // The dropped total already emitted as a self-metric; CAS-guarded so one thread reports.
    private final AtomicLong reportedDropped = new AtomicLong();
    // Self-metrics facade (cadence/event policy): resolved at emission time so it observes
    // late install()s; ignored when no binding is installed or the source is disabled.
    private final MetricsOut selfOut = Metrics.forSource(QueueMetrics.METRICS_SOURCE);
    // Reused DTO for the dropped self-metric; per-thread, as the method writers are.
    private final ThreadLocal<CounterMetric> droppedMetric;
    // The handle from install(); closed (uninstalled) by close(). Null when not installed.
    @Nullable
    private volatile Metrics.Installation installation;

    /**
     * Creates a binding over the given metrics queue with every source enabled.
     * The queue remains owned (and closed) by the caller.
     *
     * @param queue the dedicated metrics queue to write to
     * @throws IllegalStateException if only a proxy method writer is available and
     *                               {@value #ALLOW_PROXY_WRITER_PROPERTY} is not set
     */
    public QueueMetricsBinding(@NotNull ChronicleQueue queue) {
        this(queue, null, false);
    }

    /**
     * Creates a binding over the given metrics queue with log-level-style source selection.
     * The queue remains owned (and closed) by the caller.
     *
     * @param queue     the dedicated metrics queue to write to
     * @param selection which sources are enabled, or {@code null} for all
     * @throws IllegalStateException if only a proxy method writer is available and
     *                               {@value #ALLOW_PROXY_WRITER_PROPERTY} is not set
     */
    public QueueMetricsBinding(@NotNull ChronicleQueue queue, @Nullable SourceSelection selection) {
        this(queue, selection, false);
    }

    private QueueMetricsBinding(@NotNull ChronicleQueue queue, @Nullable SourceSelection selection, boolean ownsQueue) {
        this.queue = queue;
        this.selection = selection;
        this.ownsQueue = ownsQueue;
        final String queueLabel = QueueMetrics.queueLabel(queue.file() == null ? null : queue.file().getName());
        this.droppedMetric = ThreadLocal.withInitial(() -> new CounterMetric()
                .source(QueueMetrics.METRICS_SOURCE)
                .name(QueueMetrics.DROPPED_TOTAL)
                .labels(queueLabel));
        try {
            writeFormatHeader();
            // Fail fast here, not on a recording thread, if codegen is unavailable.
            writers.get();
        } catch (Throwable t) {
            if (ownsQueue)
                net.openhft.chronicle.core.io.Closeable.closeQuietly(queue);
            throw Jvm.rethrow(t);
        }
    }

    /**
     * Creates a binding over a new metrics queue in the given directory, with the design's
     * default short roll cycle ({@link RollCycles#FAST_HOURLY}) and every source enabled.
     * The queue is owned by the binding and closed with it.
     *
     * @param path the dedicated metrics queue directory, e.g. {@code <run-dir>/metrics/<serviceId>}
     * @return the new binding
     */
    public static QueueMetricsBinding at(@NotNull File path) {
        return at(path, null);
    }

    /**
     * Creates a binding over a new metrics queue in the given directory, with the design's
     * default short roll cycle ({@link RollCycles#FAST_HOURLY}) and the given source selection.
     * The queue is owned by the binding and closed with it.
     *
     * @param path      the dedicated metrics queue directory
     * @param selection which sources are enabled, or {@code null} for all
     * @return the new binding
     */
    public static QueueMetricsBinding at(@NotNull File path, @Nullable SourceSelection selection) {
        return at(path, selection, RollCycles.FAST_HOURLY);
    }

    /**
     * Creates a binding over a new metrics queue in the given directory, with the given roll
     * cycle - the retention granularity of the metrics queue, see the class javadoc - and the
     * given source selection. The queue is owned by the binding and closed with it.
     *
     * @param path      the dedicated metrics queue directory
     * @param selection which sources are enabled, or {@code null} for all
     * @param rollCycle the metrics queue's roll cycle, e.g. {@link RollCycles#FAST_HOURLY}
     * @return the new binding
     */
    public static QueueMetricsBinding at(@NotNull File path, @Nullable SourceSelection selection,
                                         @NotNull RollCycle rollCycle) {
        ChronicleQueue queue = SingleChronicleQueueBuilder.single(path)
                .rollCycle(rollCycle)
                .build();
        try {
            return new QueueMetricsBinding(queue, selection, true);
        } catch (Throwable t) {
            net.openhft.chronicle.core.io.Closeable.closeQuietly(queue);
            throw Jvm.rethrow(t);
        }
    }

    /**
     * Installs this binding process-wide via {@link Metrics#install(MetricsBinding)},
     * retaining the {@link Metrics.Installation} handle so {@link #close()} uninstalls it
     * again (restoring whatever binding was installed before).
     *
     * @return this instance for chaining
     */
    public QueueMetricsBinding install() {
        this.installation = Metrics.install(this);
        return this;
    }

    @Override
    public MetricsOut outFor(String source) {
        if (selection != null && !selection.enabled(source))
            return null;
        return out;
    }

    /**
     * Returns the metrics queue this binding writes to; exposed so a same-host consumer or a
     * test can tail it.
     *
     * @return the metrics queue
     */
    @NotNull
    public ChronicleQueue queue() {
        return queue;
    }

    /**
     * Returns the number of metric events dropped because a write failed. Thread-safe; also
     * emitted as the {@value QueueMetrics#DROPPED_TOTAL} self-metric, see the class javadoc.
     *
     * @return the dropped-event count
     */
    public long droppedCount() {
        return dropped.sum();
    }

    /**
     * If this binding was {@linkplain #install() installed}, uninstalls it (restoring the
     * previously installed binding) so the global {@code Metrics} never routes to a closed
     * binding; then closes the queue if this binding owns it.
     */
    @Override
    public void close() {
        Metrics.Installation installation = this.installation;
        this.installation = null;
        if (installation != null)
            installation.close();
        if (ownsQueue)
            queue.close();
    }

    /**
     * Writes the {@code metricsFormat} header event if the queue has no excerpts yet, per the
     * P3 format discipline: the queue's first excerpt identifies the schema. Fails
     * construction if the header cannot be written; a metrics queue without its first-excerpt
     * header is not a valid producer queue for the gateway.
     */
    private void writeFormatHeader() {
        try {
            if (queue.firstIndex() != Long.MAX_VALUE)
                return; // not empty: the header is already the first excerpt
            queue.methodWriter(MetricsFormatListener.class).metricsFormat(new MetricsFormat());
        } catch (Throwable t) {
            onWriteFailure(t);
            throw new IllegalStateException("Unable to write metricsFormat header to "
                    + queue.fileAbsolutePath(), t);
        }
    }

    private MetricsOut newWriter() {
        MetricsOut writer = queue.methodWriter(MetricsOut.class);
        // Wire's proxy-based method writers allocate an Object[] per invocation; the queue
        // binding expects the generated (compile-time) writer on this path - see design 5.3.
        if (Proxy.isProxyClass(writer.getClass())) {
            if (!allowProxyWriter)
                throw new IllegalStateException(
                        "Generated MetricsOut method writer is unavailable and only a " +
                                "java.lang.reflect.Proxy fallback (which allocates per event) could be " +
                                "created; refusing to record metrics through it. Set -D" +
                                ALLOW_PROXY_WRITER_PROPERTY + "=true to permit the allocating fallback.");
            Jvm.warn().on(QueueMetricsBinding.class,
                    "Falling back to a proxy method writer for MetricsOut; " +
                            "the recording path will allocate per event");
        }
        return writer;
    }

    // Test seam: the calling thread's method writer, creating it if needed.
    MetricsOut writerForCurrentThread() {
        return writers.get();
    }

    // Test seam and single failure funnel: count the drop, warn once.
    void onWriteFailure(Throwable t) {
        dropped.increment();
        if (warnedOnce.compareAndSet(false, true))
            Jvm.warn().on(QueueMetricsBinding.class,
                    "Failed to write metric event to " + queue.fileAbsolutePath()
                            + "; dropping, further drops are counted but not logged", t);
    }

    /**
     * Emits the {@value QueueMetrics#DROPPED_TOTAL} self-metric if drops occurred since the
     * last report. Called after each successful write (the binding's write cadence); the
     * facade is checked at emission time per the cadence/event policy, so this observes late
     * installs and runtime toggles. If the emission itself fails it is counted as a drop and
     * retried after the next successful write.
     */
    private void maybeReportDropped() {
        final long reported = reportedDropped.get();
        if (reported == REPORTING_DROPPED)
            return;
        final long total = dropped.sum();
        if (total == reported)
            return;
        if (ThreadLocalisedMetricsOut.unwrap(selfOut) instanceof IgnoresEverything)
            return;
        if (!reportedDropped.compareAndSet(reported, REPORTING_DROPPED))
            return;
        try {
            selfOut.counterMetric(droppedMetric.get()
                    .count(total)
                    .delta(total - reported)
                    .eventTime(ServicesTimestampLongConverter.currentTime())
                    .intervalNs(0));
            reportedDropped.set(total);
        } catch (Throwable t) {
            reportedDropped.compareAndSet(REPORTING_DROPPED, reported);
            onWriteFailure(t);
        }
    }

    /**
     * The {@link MetricsOut} handed to sources: delegates each event to the calling thread's
     * method writer, converting any write failure into a drop (count + one-time warn), and
     * reporting accumulated drops as a self-metric after each successful write.
     */
    final class DropNotThrowOut implements MetricsOut {

        @Override
        public void counterMetric(CounterMetric metric) {
            try {
                writers.get().counterMetric(metric);
            } catch (Throwable t) {
                onWriteFailure(t);
                return;
            }
            maybeReportDropped();
        }

        @Override
        public void gaugeMetric(GaugeMetric metric) {
            try {
                writers.get().gaugeMetric(metric);
            } catch (Throwable t) {
                onWriteFailure(t);
                return;
            }
            maybeReportDropped();
        }

        @Override
        public void histogramMetric(HistogramMetric metric) {
            try {
                writers.get().histogramMetric(metric);
            } catch (Throwable t) {
                onWriteFailure(t);
                return;
            }
            maybeReportDropped();
        }

        @Override
        public void rateMetric(RateMetric metric) {
            try {
                writers.get().rateMetric(metric);
            } catch (Throwable t) {
                onWriteFailure(t);
                return;
            }
            maybeReportDropped();
        }

        @Override
        public void pointEvent(PointEvent metric) {
            try {
                writers.get().pointEvent(metric);
            } catch (Throwable t) {
                onWriteFailure(t);
                return;
            }
            maybeReportDropped();
        }
    }
}
