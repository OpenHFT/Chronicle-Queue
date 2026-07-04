/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.metrics;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.metrics.CounterMetric;
import net.openhft.chronicle.wire.metrics.HistogramMetric;
import net.openhft.chronicle.wire.metrics.Metrics;
import net.openhft.chronicle.wire.metrics.MetricsBinding;
import net.openhft.chronicle.wire.metrics.MetricsOut;
import net.openhft.chronicle.wire.metrics.MetricsRegistry;
import net.openhft.chronicle.wire.metrics.SourceSelection;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Asserts the queue's own touch points: with a capturing {@link MetricsBinding} installed
 * <em>before</em> queue construction (the resolve-once policy), appending to a queue records
 * the {@code chronicle.queue.appender} appends counter and write-latency histogram - each
 * labelled {@code queue=<directory-basename>} - and the {@link MetricsStoreFileListener} seam
 * counts rolls. Also pins down the latency-coverage contract: outermost-commit-only for
 * nested contexts, nothing for metadata, rollback and the raw writeBytes path.
 */
public class AppenderMetricsTest extends QueueTestCommon {

    private static final long EVENT_TIME = 1_000_000_000L;
    private static final long SECOND_NS = 1_000_000_000L;

    @After
    public void resetMetrics() {
        Metrics.resetForTesting();
    }

    /**
     * The appender here is created AFTER install, so it records (review #56's positive half).
     */
    @Test
    public void appendingRecordsAppendsCounterAndWriteLatencyWithQueueLabel() {
        CapturingMetricsOut capture = new CapturingMetricsOut();
        installCapturing(capture);

        File dir = getTmpDir();
        String expectedLabels = "queue=" + dir.getName();
        MetricsRegistry registry = Metrics.registry(QueueMetrics.APPENDER_SOURCE);
        try (ChronicleQueue queue = single(dir).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender()) {

            // baseline flush so the deltas below are exactly this test's appends
            registry.flush(new CapturingMetricsOut(), EVENT_TIME, SECOND_NS);

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("hello").text("world-" + i);
                }
            }

            registry.flush(capture, EVENT_TIME + SECOND_NS, SECOND_NS);
        }

        CounterMetric appends = capture.lastMetric("counterMetric", QueueMetrics.APPENDS_TOTAL);
        assertNotNull("expected a " + QueueMetrics.APPENDS_TOTAL + " counter", appends);
        assertEquals(QueueMetrics.APPENDER_SOURCE, appends.source());
        assertEquals(QueueMetrics.APPENDS_TOTAL, appends.name());
        assertEquals(expectedLabels, appends.labels());
        assertEquals(dir.getName(), appends.decodeLabels().get("queue"));
        assertEquals(5, appends.delta());
        assertTrue("count includes this test's appends", appends.count() >= 5);
        assertEquals(EVENT_TIME + SECOND_NS, appends.eventTime());
        assertEquals(SECOND_NS, appends.intervalNs());

        HistogramMetric writeLatency = capture.lastMetric("histogramMetric", QueueMetrics.WRITE_LATENCY_NS);
        assertNotNull("expected a " + QueueMetrics.WRITE_LATENCY_NS + " histogram", writeLatency);
        assertEquals(QueueMetrics.APPENDER_SOURCE, writeLatency.source());
        assertEquals(expectedLabels, writeLatency.labels());
        assertEquals(5, writeLatency.histogram().count());
    }

    /**
     * Two appenders on the same queue share the same per-queue instruments (registration
     * dedup on kind, name, labels) - review #2/#51.
     */
    @Test
    public void twoAppendersOnOneQueueShareInstruments() {
        installCapturing(new CapturingMetricsOut());

        File dir = getTmpDir();
        try (ChronicleQueue queue = single(dir).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender()) {
            assertNotNull(appender);
            assertSame(QueueMetrics.appends(dir.getName()), QueueMetrics.appends(dir.getName()));
            assertSame(QueueMetrics.writeLatency(dir.getName()), QueueMetrics.writeLatency(dir.getName()));
        }
    }

    /**
     * Review #54: nested writingDocument() records exactly one append and one latency sample,
     * at the outermost commit.
     */
    @Test
    public void nestedWritingDocumentRecordsLatencyOnceAtOutermostCommit() {
        CapturingMetricsOut capture = new CapturingMetricsOut();
        installCapturing(capture);

        MetricsRegistry registry = Metrics.registry(QueueMetrics.APPENDER_SOURCE);
        try (ChronicleQueue queue = single(getTmpDir()).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender()) {

            registry.flush(new CapturingMetricsOut(), EVENT_TIME, SECOND_NS);

            try (DocumentContext outer = appender.writingDocument()) {
                outer.wire().write("outer").text("payload");
                try (DocumentContext inner = appender.writingDocument()) {
                    inner.wire().write("nested").text("payload");
                }
                // inner close must not commit or record; only the outermost close commits
            }

            registry.flush(capture, EVENT_TIME + SECOND_NS, SECOND_NS);
        }

        CounterMetric appends = capture.lastMetric("counterMetric", QueueMetrics.APPENDS_TOTAL);
        assertNotNull(appends);
        assertEquals(1, appends.delta());

        HistogramMetric writeLatency = capture.lastMetric("histogramMetric", QueueMetrics.WRITE_LATENCY_NS);
        assertNotNull(writeLatency);
        assertEquals(1, writeLatency.histogram().count());
    }

    /**
     * Review #55: metadata writes count neither appends nor latency.
     */
    @Test
    public void metadataWritesCountNeitherAppendsNorLatency() {
        CapturingMetricsOut capture = new CapturingMetricsOut();
        installCapturing(capture);

        MetricsRegistry registry = Metrics.registry(QueueMetrics.APPENDER_SOURCE);
        try (ChronicleQueue queue = single(getTmpDir()).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender()) {

            registry.flush(new CapturingMetricsOut(), EVENT_TIME, SECOND_NS);

            try (DocumentContext dc = appender.writingDocument(true)) {
                dc.wire().write("meta").text("data");
            }

            registry.flush(capture, EVENT_TIME + SECOND_NS, SECOND_NS);
        }

        CounterMetric appends = capture.lastMetric("counterMetric", QueueMetrics.APPENDS_TOTAL);
        assertNotNull(appends);
        assertEquals(0, appends.delta());

        HistogramMetric writeLatency = capture.lastMetric("histogramMetric", QueueMetrics.WRITE_LATENCY_NS);
        assertNotNull(writeLatency);
        assertEquals(0, writeLatency.histogram().count());
    }

    /**
     * Review #6/#53: a rolled-back context clears the latency start, so a later commit that
     * did not come through writingDocument() (the raw writeBytes path) cannot record a bogus
     * open-to-commit latency from the stale timestamp.
     */
    @Test
    public void rollbackClearsLatencyStartSoNextCommitIsNotMisTimed() {
        CapturingMetricsOut capture = new CapturingMetricsOut();
        installCapturing(capture);

        MetricsRegistry registry = Metrics.registry(QueueMetrics.APPENDER_SOURCE);
        Bytes<?> payload = Bytes.from("raw-payload");
        try (ChronicleQueue queue = single(getTmpDir()).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender()) {

            registry.flush(new CapturingMetricsOut(), EVENT_TIME, SECOND_NS);

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("doomed").text("write");
                dc.rollbackOnClose();
            }

            // the raw writeBytes path counts an append but must not sample latency -
            // and especially not the stale start of the rolled-back context above
            appender.writeBytes(payload);

            registry.flush(capture, EVENT_TIME + SECOND_NS, SECOND_NS);
        } finally {
            payload.releaseLast();
        }

        CounterMetric appends = capture.lastMetric("counterMetric", QueueMetrics.APPENDS_TOTAL);
        assertNotNull(appends);
        assertEquals("rollback records nothing; writeBytes records one append", 1, appends.delta());

        HistogramMetric writeLatency = capture.lastMetric("histogramMetric", QueueMetrics.WRITE_LATENCY_NS);
        assertNotNull(writeLatency);
        assertEquals("neither the rollback nor the raw writeBytes path samples latency",
                0, writeLatency.histogram().count());
    }

    /**
     * Review #52 (documented choice): the raw writeBytes(BytesStore) path is appends-only -
     * counted, never latency-sampled. See the StoreAppender and QueueMetrics javadoc.
     */
    @Test
    public void writeBytesPathCountsAppendsButNotLatency() {
        CapturingMetricsOut capture = new CapturingMetricsOut();
        installCapturing(capture);

        MetricsRegistry registry = Metrics.registry(QueueMetrics.APPENDER_SOURCE);
        Bytes<?> payload = Bytes.from("raw-payload");
        try (ChronicleQueue queue = single(getTmpDir()).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender()) {

            registry.flush(new CapturingMetricsOut(), EVENT_TIME, SECOND_NS);

            appender.writeBytes(payload);
            appender.writeBytes(payload);

            registry.flush(capture, EVENT_TIME + SECOND_NS, SECOND_NS);
        } finally {
            payload.releaseLast();
        }

        CounterMetric appends = capture.lastMetric("counterMetric", QueueMetrics.APPENDS_TOTAL);
        assertNotNull(appends);
        assertEquals(2, appends.delta());

        HistogramMetric writeLatency = capture.lastMetric("histogramMetric", QueueMetrics.WRITE_LATENCY_NS);
        assertNotNull(writeLatency);
        assertEquals(0, writeLatency.histogram().count());
    }

    /**
     * Review #56: the resolve-once (forSourceStatic) policy - an appender created BEFORE the
     * binding is installed records nothing, even after a later install. Install the binding
     * before constructing the components to be measured.
     */
    @Test
    public void appenderCreatedBeforeInstallRecordsNothingEvenAfterInstall() {
        CapturingMetricsOut capture = new CapturingMetricsOut();

        try (ChronicleQueue queue = single(getTmpDir()).testBlockSize().build();
             ExcerptAppender appender = queue.createAppender()) {

            // installed only after the queue and appender were constructed
            installCapturing(capture);

            for (int i = 0; i < 3; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("hello").text("world-" + i);
                }
            }

            Metrics.registry(QueueMetrics.APPENDER_SOURCE).flush(capture, EVENT_TIME, SECOND_NS);
        }

        assertNull("appender constructed before install must not register instruments",
                capture.lastMetric("counterMetric", QueueMetrics.APPENDS_TOTAL));
        assertNull(capture.lastMetric("histogramMetric", QueueMetrics.WRITE_LATENCY_NS));
    }

    @Test
    public void sharedPerQueueInstrumentsTolerateConcurrentAppendAndFlush() throws Exception {
        installCapturing(new CapturingMetricsOut());

        File dir = getTmpDir();
        MetricsRegistry registry = Metrics.registry(QueueMetrics.APPENDER_SOURCE);
        CapturingMetricsOut baseline = new CapturingMetricsOut();
        final int writerThreads = 4;
        final int appendsPerThread = 25;

        try (ChronicleQueue queue = single(dir).testBlockSize().build()) {
            registry.flush(baseline, EVENT_TIME, SECOND_NS);
            CounterMetric baselineAppends = baseline.lastMetric("counterMetric", QueueMetrics.APPENDS_TOTAL);
            final long baseCount = baselineAppends == null ? 0 : baselineAppends.count();
            final CountDownLatch start = new CountDownLatch(1);
            final AtomicBoolean done = new AtomicBoolean();
            ExecutorService executor = Executors.newFixedThreadPool(writerThreads + 1);
            try {
                Future<?> flusher = executor.submit(() -> {
                    await(start);
                    while (!done.get())
                        registry.flush(new CapturingMetricsOut(), EVENT_TIME, SECOND_NS);
                });
                List<Future<?>> writers = new ArrayList<>();
                for (int t = 0; t < writerThreads; t++) {
                    final int thread = t;
                    writers.add(executor.submit(() -> {
                        await(start);
                        try (ExcerptAppender appender = queue.createAppender()) {
                            for (int i = 0; i < appendsPerThread; i++) {
                                try (DocumentContext dc = appender.writingDocument()) {
                                    dc.wire().write("writer").int32(thread)
                                            .write("seq").int32(i);
                                }
                            }
                        }
                    }));
                }

                start.countDown();
                for (Future<?> writer : writers)
                    writer.get();
                done.set(true);
                flusher.get();
            } finally {
                done.set(true);
                executor.shutdownNow();
                assertTrue("executor did not stop", executor.awaitTermination(5, TimeUnit.SECONDS));
            }

            CapturingMetricsOut capture = new CapturingMetricsOut();
            registry.flush(capture, EVENT_TIME + SECOND_NS, SECOND_NS);
            CounterMetric appends = capture.lastMetric("counterMetric", QueueMetrics.APPENDS_TOTAL);
            assertNotNull(appends);
            assertEquals(writerThreads * appendsPerThread, appends.count() - baseCount);
        }
    }

    @Test
    public void storeFileListenerSeamCountsRollsWithQueueLabel() {
        CapturingMetricsOut capture = new CapturingMetricsOut();
        installCapturing(capture);

        StoreFileListener wrapped = MetricsStoreFileListener.wrap(StoreFileListener.noOp(), "rolls-queue");
        assertTrue(wrapped instanceof MetricsStoreFileListener);
        assertTrue(wrapped.isActive());

        MetricsRegistry registry = Metrics.registry(QueueMetrics.APPENDER_SOURCE);
        registry.flush(new CapturingMetricsOut(), EVENT_TIME, SECOND_NS);

        // synchronous stand-in for the queue's background onAcquired callback
        wrapped.onAcquired(1, new File("ignored"));   // first open: roll 1
        wrapped.onReleased(1, new File("ignored"));
        wrapped.onAcquired(2, new File("ignored"));   // forward roll: roll 2

        // NOT rolls: tailers replaying old cycles, metadata queries and current-cycle
        // re-acquisitions all fire onAcquired too and must not inflate the counter
        wrapped.onAcquired(1, new File("ignored"));   // tailer replaying an old cycle
        wrapped.onAcquired(2, new File("ignored"));   // current-cycle re-acquisition
        wrapped.onReleased(2, new File("ignored"));

        registry.flush(capture, EVENT_TIME + SECOND_NS, SECOND_NS);

        CounterMetric rolls = capture.lastMetric("counterMetric", QueueMetrics.ROLLS_TOTAL);
        assertNotNull("expected a " + QueueMetrics.ROLLS_TOTAL + " counter", rolls);
        assertEquals(QueueMetrics.APPENDER_SOURCE, rolls.source());
        assertEquals("queue=rolls-queue", rolls.labels());
        assertEquals("only the first open and the forward roll count", 2, rolls.delta());

        // an idle gap across several cycles is one transition, not three
        wrapped.onAcquired(5, new File("ignored"));
        registry.flush(capture, EVENT_TIME + 2 * SECOND_NS, SECOND_NS);
        rolls = capture.lastMetric("counterMetric", QueueMetrics.ROLLS_TOTAL);
        assertEquals(1, rolls.delta());
    }

    /**
     * Review #57: the roll-listener seam applies the binding's source selection at
     * construction time - selection disabling {@code chronicle.queue.appender} leaves the
     * listener unwrapped, and enabling it wraps, both decided when wrap() runs.
     */
    @Test
    public void rollListenerRespectsSourceSelectionAtConstructionTime() {
        CapturingMetricsOut capture = new CapturingMetricsOut();
        SourceSelection disabled = new SourceSelection(
                Collections.singletonMap(QueueMetrics.APPENDER_SOURCE, Boolean.FALSE));
        Metrics.install(new MetricsBinding() {
            @Override
            public MetricsOut outFor(String source) {
                return disabled.enabled(source) ? capture : null;
            }
        });

        assertFalse(QueueMetrics.appenderEnabled());
        StoreFileListener noOp = StoreFileListener.noOp();
        assertSame("selection disabled at construction: no wrapping",
                noOp, MetricsStoreFileListener.wrap(noOp, "selected-queue"));

        // now enabled: constructed-from-now-on listeners are wrapped
        installCapturing(capture);
        assertTrue(QueueMetrics.appenderEnabled());
        assertTrue(MetricsStoreFileListener.wrap(noOp, "selected-queue")
                instanceof MetricsStoreFileListener);
    }

    @Test
    public void touchPointsAreInertWithoutABinding() {
        Metrics.resetForTesting();
        assertFalse(QueueMetrics.appenderEnabled());

        StoreFileListener noOp = StoreFileListener.noOp();
        assertSame(noOp, MetricsStoreFileListener.wrap(noOp, "any-queue"));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private void installCapturing(final CapturingMetricsOut capture) {
        Metrics.install(new MetricsBinding() {
            @Override
            public MetricsOut outFor(String source) {
                return capture;
            }
        });
    }
}
