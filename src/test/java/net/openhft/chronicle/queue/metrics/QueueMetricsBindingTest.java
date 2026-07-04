/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.core.util.IgnoresEverything;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import net.openhft.chronicle.wire.metrics.CounterMetric;
import net.openhft.chronicle.wire.metrics.GaugeMetric;
import net.openhft.chronicle.wire.metrics.HistogramMetric;
import net.openhft.chronicle.wire.metrics.MetricHistogram;
import net.openhft.chronicle.wire.metrics.Metrics;
import net.openhft.chronicle.wire.metrics.MetricsOut;
import net.openhft.chronicle.wire.metrics.PointEvent;
import net.openhft.chronicle.wire.metrics.RateMetric;
import net.openhft.chronicle.wire.metrics.SourceSelection;
import net.openhft.chronicle.wire.metrics.ThreadLocalisedMetricsOut;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Proxy;
import java.util.Collections;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Writes metric events through a {@link QueueMetricsBinding} to a temp queue, tails it back
 * with a {@code MethodReader} into a capturing {@code MetricsOut}, and asserts the
 * {@code metricsFormat} header event plus a round-trip of every metric kind.
 */
public class QueueMetricsBindingTest extends QueueTestCommon {

    private static final long EVENT_TIME = 1_000_000_000L;
    private static final long SECOND_NS = 1_000_000_000L;

    @After
    public void resetMetrics() {
        Metrics.resetForTesting();
    }

    @Test
    public void writesHeaderAndRoundTripsEveryMetricKind() {
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir())) {
            MetricsOut out = binding.outFor("chronicle.queue.test");
            assertNotNull(out);

            // build once, mutate and re-emit - as instruments do
            CounterMetric counter = new CounterMetric()
                    .source("chronicle.queue.test")
                    .name("chronicle_queue_test_events_total")
                    .label("queue", "orders-out");
            out.counterMetric(counter.count(4711).delta(2)
                    .eventTime(EVENT_TIME).intervalNs(SECOND_NS));

            GaugeMetric gauge = new GaugeMetric()
                    .source("chronicle.queue.test")
                    .name("chronicle_queue_test_lag_bytes");
            out.gaugeMetric(gauge.value(42.5)
                    .eventTime(EVENT_TIME).intervalNs(SECOND_NS));

            Histogram samples = new Histogram();
            for (int i = 1; i <= 100; i++)
                samples.sampleNanos(i * 1000L);
            HistogramMetric histogram = new HistogramMetric()
                    .histogram(new MetricHistogram())
                    .source("chronicle.queue.test")
                    .name("chronicle_queue_test_write_latency_ns");
            histogram.histogram().sampleFrom(samples);
            out.histogramMetric(histogram
                    .eventTime(EVENT_TIME).intervalNs(SECOND_NS));

            RateMetric rate = new RateMetric()
                    .source("chronicle.queue.test")
                    .name("chronicle_queue_test_events_rate");
            out.rateMetric(rate.count(360).perSecond(3.5)
                    .eventTime(EVENT_TIME).intervalNs(SECOND_NS));

            PointEvent point = new PointEvent()
                    .source("chronicle.queue.test")
                    .name("chronicle_queue_test_reconnects_total");
            out.pointEvent(point.value(1).eventTime(EVENT_TIME + SECOND_NS));

            assertEquals(0, binding.droppedCount());

            CapturingMetricsOut capture = tail(binding);
            assertEquals(6, capture.size());

            // the header is the queue's first excerpt
            assertEquals("metricsFormat", capture.methods.get(0));
            MetricsFormat format = capture.event(0);
            assertEquals(MetricsFormat.MAGIC, format.magic());
            assertEquals("chronicle-metrics", format.magic());
            assertEquals(1, format.formatVersion());
            assertTrue(format.compatible());

            // each metric kind round-trips, discriminated by method name
            assertEquals("counterMetric", capture.methods.get(1));
            CounterMetric counter1 = capture.event(1);
            assertEquals("chronicle.queue.test", counter1.source());
            assertEquals("chronicle_queue_test_events_total", counter1.name());
            assertEquals("queue=orders-out", counter1.labels());
            assertEquals("orders-out", counter1.decodeLabels().get("queue"));
            assertEquals(4711, counter1.count());
            assertEquals(2, counter1.delta());
            assertEquals(EVENT_TIME, counter1.eventTime());
            assertEquals(SECOND_NS, counter1.intervalNs());

            assertEquals("gaugeMetric", capture.methods.get(2));
            GaugeMetric gauge2 = capture.event(2);
            assertEquals("chronicle_queue_test_lag_bytes", gauge2.name());
            assertEquals(42.5, gauge2.value(), 0.0);

            assertEquals("histogramMetric", capture.methods.get(3));
            HistogramMetric histogram3 = capture.event(3);
            assertEquals("chronicle_queue_test_write_latency_ns", histogram3.name());
            assertEquals(100, histogram3.histogram().count());

            assertEquals("rateMetric", capture.methods.get(4));
            RateMetric rate4 = capture.event(4);
            assertEquals(360, rate4.count());
            assertEquals(3.5, rate4.perSecond(), 0.0);

            assertEquals("pointEvent", capture.methods.get(5));
            PointEvent point5 = capture.event(5);
            assertEquals(1.0, point5.value(), 0.0);
            assertEquals(0L, point5.intervalNs());
            assertEquals(EVENT_TIME + SECOND_NS, point5.eventTime());
        }
    }

    @Test
    public void headerIsWrittenOnlyForAnEmptyQueue() {
        File dir = getTmpDir();
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(dir)) {
            binding.outFor("a.b").pointEvent(new PointEvent()
                    .source("a.b").name("a_b_total").value(1).eventTime(EVENT_TIME));
        }
        // reopening over the same directory must not prepend a second header
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(dir)) {
            CapturingMetricsOut capture = tail(binding);
            assertEquals(2, capture.size());
            assertEquals("metricsFormat", capture.methods.get(0));
            assertEquals("pointEvent", capture.methods.get(1));
        }
    }

    @Test
    public void rejectsExistingQueueWithoutMetricsFormatHeader() {
        File dir = getTmpDir();
        try (ChronicleQueue queue = single(dir).testBlockSize().rollCycle(RollCycles.FAST_HOURLY).build()) {
            queue.methodWriter(MetricsOut.class).pointEvent(new PointEvent()
                    .source("not.metrics").name("not_metrics_total").value(1).eventTime(EVENT_TIME));
        }

        try {
            QueueMetricsBinding binding = QueueMetricsBinding.at(dir);
            binding.close();
            fail("expected an existing non-metrics queue to be rejected");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("expected metricsFormat"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("pointEvent"));
        }
    }

    @Test
    public void rejectsExistingQueueWithIncompatibleMetricsFormatHeader() {
        File dir = getTmpDir();
        try (ChronicleQueue queue = single(dir).testBlockSize().rollCycle(RollCycles.FAST_HOURLY).build()) {
            queue.methodWriter(MetricsFormatListener.class).metricsFormat(
                    new MetricsFormat().formatVersion(MetricsFormat.FORMAT_VERSION + 1));
        }

        try {
            QueueMetricsBinding binding = QueueMetricsBinding.at(dir);
            binding.close();
            fail("expected an incompatible metrics queue to be rejected");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("incompatible metricsFormat"));
        }
    }

    @Test
    public void writeFailuresAreCountedAndNeverThrown() {
        expectException("Failed to write metric event");
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir())) {
            MetricsOut out = binding.outFor("chronicle.queue.test");
            PointEvent point = new PointEvent()
                    .source("chronicle.queue.test").name("a_total");
            out.pointEvent(point.value(1).eventTime(EVENT_TIME));
            assertEquals(0, binding.droppedCount());

            binding.queue().close();

            // must not throw into the caller; counted, warned once
            out.pointEvent(point.value(2).eventTime(EVENT_TIME));
            assertEquals(1, binding.droppedCount());
            out.counterMetric(new CounterMetric().source("chronicle.queue.test")
                    .name("b_total").count(1).delta(1).eventTime(EVENT_TIME));
            assertEquals(2, binding.droppedCount());
        }
    }

    @Test
    public void sourceSelectionDisablesSources() {
        SourceSelection selection = new SourceSelection(
                Collections.singletonMap("chronicle.queue.tailer", Boolean.FALSE));
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir(), selection)) {
            assertNull(binding.outFor("chronicle.queue.tailer"));
            assertNull(binding.outFor("chronicle.queue.tailer.lag"));
            assertNotNull(binding.outFor("chronicle.queue.appender"));
            assertSame(binding.outFor("chronicle.queue.appender"), binding.outFor("chronicle.fix.session"));
        }
    }

    /**
     * Review #7: a binding installed via its install() convenience uninstalls itself on
     * close(), so the global Metrics never routes to a closed binding.
     */
    @Test
    public void closeUninstallsABindingInstalledViaInstall() {
        expectStaticAppenderResolveOnceWarning();
        QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir()).install();
        MetricsOut facade = Metrics.forSource("chronicle.queue.test");
        assertFalse("installed: the facade must route to the binding",
                ThreadLocalisedMetricsOut.unwrap(facade) instanceof IgnoresEverything);

        binding.close();

        assertTrue("closed: the facade must not route to the closed binding",
                ThreadLocalisedMetricsOut.unwrap(facade) instanceof IgnoresEverything);
        // and emitting through a cached facade after close is a safe no-op
        facade.pointEvent(new PointEvent()
                .source("chronicle.queue.test").name("after_close_total").value(1).eventTime(EVENT_TIME));
    }

    @Test
    public void installRejectsRepeatedInstallOnTheSameBinding() {
        expectStaticAppenderResolveOnceWarning();
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir()).install()) {
            try {
                binding.install();
                fail("expected repeated install() to be rejected");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("already installed"));
            }
        }
    }

    /**
     * Review #7: close() restores whatever binding was installed before install().
     */
    @Test
    public void closeRestoresThePreviouslyInstalledBinding() {
        CapturingMetricsOut previous = new CapturingMetricsOut();
        Metrics.install(source -> previous);

        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir()).install()) {
            assertSame(binding.outFor("a.b"), ThreadLocalisedMetricsOut.unwrap(Metrics.forSource("a.b")));
            assertNotSame(previous, ThreadLocalisedMetricsOut.unwrap(Metrics.forSource("a.b")));
        }

        assertSame(previous, ThreadLocalisedMetricsOut.unwrap(Metrics.forSource("a.b")));
    }

    /**
     * Review #5/#27: the production recording path uses the generated method writer, not
     * Wire's allocating java.lang.reflect.Proxy fallback.
     */
    @Test
    public void recordingPathUsesAGeneratedMethodWriterNotAProxy() {
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir())) {
            // production path: record through the binding, then inspect the writer it used
            binding.outFor("chronicle.queue.test").pointEvent(new PointEvent()
                    .source("chronicle.queue.test").name("a_total").value(1).eventTime(EVENT_TIME));
            assertEquals(0, binding.droppedCount());

            MetricsOut writer = binding.writerForCurrentThread();
            assertFalse("expected the generated (compile-time) method writer, got a Proxy",
                    Proxy.isProxyClass(writer.getClass()));
        }
    }

    /**
     * Review #27: when only a proxy writer can be created, construction fails fast by
     * default (allowProxyWriter=false) instead of warn-and-allocate-per-event.
     */
    @Test
    public void failsFastWhenOnlyAProxyWriterIsAvailable() {
        expectException("Falling back to proxy method writer");
        System.setProperty(VanillaMethodWriterBuilder.DISABLE_WRITER_PROXY_CODEGEN, "true");
        try {
            QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir());
            binding.close();
            fail("expected construction to fail fast without a generated method writer");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(),
                    expected.getMessage().contains(QueueMetricsBinding.ALLOW_PROXY_WRITER_PROPERTY));
        } finally {
            System.clearProperty(VanillaMethodWriterBuilder.DISABLE_WRITER_PROXY_CODEGEN);
        }
    }

    /**
     * Review #27: the escape hatch - allowProxyWriter=true accepts the allocating proxy
     * fallback with a warning.
     */
    @Test
    public void proxyWriterPermittedWhenExplicitlyAllowed() {
        expectException("Falling back to proxy method writer");
        expectException("Falling back to a proxy method writer");
        System.setProperty(VanillaMethodWriterBuilder.DISABLE_WRITER_PROXY_CODEGEN, "true");
        System.setProperty(QueueMetricsBinding.ALLOW_PROXY_WRITER_PROPERTY, "true");
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir())) {
            assertTrue(Proxy.isProxyClass(binding.writerForCurrentThread().getClass()));
        } finally {
            System.clearProperty(VanillaMethodWriterBuilder.DISABLE_WRITER_PROXY_CODEGEN);
            System.clearProperty(QueueMetricsBinding.ALLOW_PROXY_WRITER_PROPERTY);
        }
    }

    /**
     * Review #23/#24: drops are re-exposed as the chronicle_queue_metrics_dropped_total
     * self-metric (source chronicle.queue.metrics), emitted on the binding's next successful
     * write - and not re-emitted while the count is unchanged.
     */
    @Test
    public void droppedCountIsEmittedAsASelfMetricOnTheNextSuccessfulWrite() {
        expectException("Failed to write metric event");
        expectStaticAppenderResolveOnceWarning();
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir()).install()) {
            MetricsOut out = binding.outFor("chronicle.queue.test");
            PointEvent point = new PointEvent().source("chronicle.queue.test").name("a_total");

            binding.onWriteFailure(new RuntimeException("simulated write failure"));
            assertEquals(1, binding.droppedCount());

            // the next successful write carries the self-metric behind it
            out.pointEvent(point.value(1).eventTime(EVENT_TIME));

            CapturingMetricsOut capture = tail(binding);
            assertEquals("metricsFormat", capture.methods.get(0));
            assertEquals("pointEvent", capture.methods.get(1));
            assertEquals("counterMetric", capture.methods.get(2));
            CounterMetric droppedMetric = capture.event(2);
            assertEquals(QueueMetrics.METRICS_SOURCE, droppedMetric.source());
            assertEquals(QueueMetrics.DROPPED_TOTAL, droppedMetric.name());
            assertEquals(1, droppedMetric.count());
            assertEquals(1, droppedMetric.delta());
            assertEquals(binding.queue().file().getName(), droppedMetric.decodeLabels().get("queue"));
            assertTrue(droppedMetric.eventTime() != 0);

            // unchanged dropped count: further successful writes do not re-emit it
            out.pointEvent(point.value(2).eventTime(EVENT_TIME + SECOND_NS));
            capture = tail(binding);
            assertEquals(4, capture.size());
            int droppedEvents = 0;
            for (String method : capture.methods)
                if (method.equals("counterMetric"))
                    droppedEvents++;
            assertEquals(1, droppedEvents);
        }
    }

    @Test
    public void failedSelfMetricEmissionIsRetriedAfterTheNextSuccessfulWrite() {
        expectException("Failed to write metric event");
        expectStaticAppenderResolveOnceWarning();
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir())) {
            final MetricsOut out = binding.outFor("chronicle.queue.test");
            final FailingOnceMetricsOut selfSink = new FailingOnceMetricsOut();
            Metrics.install(source -> QueueMetrics.METRICS_SOURCE.equals(source) ? selfSink : out);

            binding.onWriteFailure(new RuntimeException("simulated write failure"));
            assertEquals(1, binding.droppedCount());

            PointEvent point = new PointEvent().source("chronicle.queue.test").name("a_total");
            out.pointEvent(point.value(1).eventTime(EVENT_TIME));
            assertEquals(2, binding.droppedCount());
            assertEquals(0, selfSink.capture.size());

            out.pointEvent(point.value(2).eventTime(EVENT_TIME + SECOND_NS));
            assertEquals(1, selfSink.capture.size());
            CounterMetric droppedMetric = selfSink.capture.event(0);
            assertEquals(QueueMetrics.DROPPED_TOTAL, droppedMetric.name());
            assertEquals(2, droppedMetric.count());
            assertEquals(2, droppedMetric.delta());
        }
    }

    /**
     * Review #50: the metrics queue's roll cycle - its retention granularity - is a builder
     * option.
     */
    @Test
    public void metricsQueueRollCycleIsConfigurable() {
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir(), null, TestRollCycles.TEST_SECONDLY)) {
            assertEquals(TestRollCycles.TEST_SECONDLY, binding.queue().rollCycle());
        }
        try (QueueMetricsBinding binding = QueueMetricsBinding.at(getTmpDir())) {
            assertEquals(RollCycles.FAST_HOURLY, binding.queue().rollCycle());
        }
    }

    private void expectStaticAppenderResolveOnceWarning() {
        expectException("Metrics.install() was called after");
    }

    private static final class FailingOnceMetricsOut implements MetricsOut {
        final CapturingMetricsOut capture = new CapturingMetricsOut();
        boolean fail = true;

        @Override
        public void counterMetric(CounterMetric metric) {
            if (fail) {
                fail = false;
                throw new IllegalStateException("simulated self-metric failure");
            }
            capture.counterMetric(metric);
        }

        @Override
        public void gaugeMetric(GaugeMetric metric) {
            capture.gaugeMetric(metric);
        }

        @Override
        public void histogramMetric(HistogramMetric metric) {
            capture.histogramMetric(metric);
        }

        @Override
        public void rateMetric(RateMetric metric) {
            capture.rateMetric(metric);
        }

        @Override
        public void pointEvent(PointEvent metric) {
            capture.pointEvent(metric);
        }
    }

    private CapturingMetricsOut tail(QueueMetricsBinding binding) {
        CapturingMetricsOut capture = new CapturingMetricsOut();
        try (ExcerptTailer tailer = binding.queue().createTailer()) {
            MethodReader reader = tailer.methodReader(capture);
            while (reader.readOne()) {
                // drain
            }
        }
        return capture;
    }
}
