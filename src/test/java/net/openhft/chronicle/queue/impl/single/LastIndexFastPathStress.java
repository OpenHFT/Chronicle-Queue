/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import com.sun.management.GarbageCollectionNotificationInfo;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.onoes.ExceptionHandler;
import net.openhft.chronicle.core.threads.CleaningThread;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.wire.DocumentContext;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * On-demand stress harness for the {@code sequenceForPosition(MAX_VALUE)} fast path in
 * {@link SCQIndexing}. <em>Not</em> a JUnit test -- run from the command line with
 *
 * <pre>{@code
 *   mvn -Dcheckstyle.skip=true test-compile
 *   java -cp target/test-classes:target/classes:$(cat classpath.txt) \
 *        net.openhft.chronicle.queue.impl.single.LastIndexFastPathStress
 * }</pre>
 *
 * Or via maven exec:
 *
 * <pre>{@code
 *   mvn test-compile exec:java \
 *     -Dexec.mainClass=net.openhft.chronicle.queue.impl.single.LastIndexFastPathStress \
 *     -Dexec.classpathScope=test
 * }</pre>
 *
 * <p>Runs for 120 seconds with one writer per ~4 cores and two readers per core. Prints a
 * one-line summary plus a per-phase max-latency breakdown, watchdog stuck-event totals,
 * GC summary, and slow-scan kind breakdown. Optional {@code -Dscq.fastpath.stress.delay.ns=N}
 * inserts a symmetric busy-wait in both writer and reader loops to widen race windows.
 *
 * <p>When JFR is available, each run dumps an allocation profile to
 * {@code /tmp/LastIndexFastPathStress-*.jfr} -- inspect with
 * {@code jfr print --events 'ObjectAllocation*' <file>}. JFR is loaded reflectively so this
 * diagnostic harness still compiles on build agents whose JDK image omits {@code jdk.jfr}.
 */
public final class LastIndexFastPathStress {

    private static final long DURATION_NANOS = TimeUnit.SECONDS.toNanos(120);
    private static final long WATCHDOG_THRESHOLD_MS = 25;
    private static final long WATCHDOG_POLL_MS = 5;

    private LastIndexFastPathStress() {
    }

    public static void main(String[] args) throws Exception {
        Path tmpDir = Files.createTempDirectory("LastIndexFastPathStress");
        boolean originalResourceTracing = Jvm.isResourceTracing();
        // Resource tracing captures a Throwable on every reference op, dominating any close-
        // phase tail and skewing the GC profile. Off for stress so we measure the real path.
        Jvm.setResourceTracing(false);
        try {
            run(tmpDir);
        } finally {
            Jvm.setResourceTracing(originalResourceTracing);
            IOTools.deleteDirWithFiles(tmpDir.toFile());
        }
    }

    private static void run(Path tmpDir) throws Exception {
        // Scale by available CPU so contention behaviour is comparable across hosts: many more
        // readers than writers so some readers stall on each tick, amplifying the chance that
        // a reader observes a writer mid-update (writePosition published, sequence half not
        // yet stored), exercising the NOT_FOUND_RETRY path.
        final int cores = Runtime.getRuntime().availableProcessors();
        final int writerCount = Math.min(4, (cores + 3) / 4);
        final int readerCount = 2 * cores;
        // Optional symmetric per-iteration delay (ns) for both writers and readers.
        final long writerDelayNs = Long.getLong("scq.fastpath.stress.delay.ns", 0L);
        final long readerDelayNs = writerDelayNs;

        // Use CleaningThread so per-thread CleaningThreadLocal cleanup hooks fire deterministically
        // when each worker thread dies at the end of the run. SCQIndexing's holder maps register a
        // close lambda; without CleaningThread that lambda would never run, leaving per-thread
        // off-heap holders to leak until the SCQIndexing itself is GC'd.
        final java.util.concurrent.atomic.AtomicInteger threadId = new java.util.concurrent.atomic.AtomicInteger();
        ExecutorService exec = Executors.newFixedThreadPool(writerCount + readerCount, r -> {
            CleaningThread t = new CleaningThread(r, "scq-fastpath-" + threadId.getAndIncrement());
            t.setDaemon(true);
            return t;
        });

        // Keep the warm appender alive for the lifetime of the run so the cycle's store (and
        // therefore its single SCQIndexing instance) stays cached. If we let it go, captured
        // counters could refer to a released instance while workers use a freshly-opened one.
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(tmpDir.toFile())
                .rollCycle(RollCycles.FAST_HOURLY)
                .build();
             ExcerptAppender warm = queue.createAppender()) {

            for (int i = 0; i < 100; i++) {
                try (DocumentContext dc = warm.writingDocument()) {
                    dc.wire().bytes().writeInt(i);
                }
            }

            final long retriesBefore = SCQIndexing.SEQ4POS_FAST_PATH_RETRIES.get();
            final long bruteForceBefore = SCQIndexing.SEQ4POS_BRUTE_FORCE_FALLTHROUGHS.get();

            final AtomicLong totalGcEvents = new AtomicLong();
            final AtomicLong totalGcMillis = new AtomicLong();
            final AtomicLong maxGcMillis = new AtomicLong();
            final long testStartMillis = System.currentTimeMillis();
            final NotificationListener gcListener = (notification, handback) -> {
                if (!notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION))
                    return;
                GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(
                        (CompositeData) notification.getUserData());
                long durMs = info.getGcInfo().getDuration();
                totalGcEvents.incrementAndGet();
                totalGcMillis.addAndGet(durMs);
                maxGcMillis.accumulateAndGet(durMs, Math::max);
                if (durMs >= 10)
                    System.out.printf("[gc] +%dms %s %s in %dms (cause=%s)%n",
                            System.currentTimeMillis() - testStartMillis,
                            info.getGcAction(), info.getGcName(), durMs, info.getGcCause());
            };
            final List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
            for (GarbageCollectorMXBean bean : gcBeans)
                ((NotificationEmitter) bean).addNotificationListener(gcListener, null, null);

            // JFR allocation profile, when the runtime has jdk.jfr available. Some CI JDK
            // images omit it, so keep this diagnostic optional and avoid a compile-time link.
            final AllocationRecording allocRecording = startAllocationRecording();

            // Recording perf handler: silently aggregate "Took X us to linearScan ..." lines so
            // the breakdown by scan kind (efficient fast-path / tail-check / brute-force fall-
            // through) is available without flooding stdout.
            final ConcurrentMap<String, AtomicLong> perfCounts = new ConcurrentHashMap<>();
            final ExceptionHandler recorder = (logger, message, throwable) -> {
                if (message != null)
                    perfCounts.computeIfAbsent(message, k -> new AtomicLong()).incrementAndGet();
            };
            Jvm.setPerfExceptionHandler(recorder);

            final AtomicBoolean stop = new AtomicBoolean(false);
            final AtomicLong totalWrites = new AtomicLong();
            final AtomicLong totalLastIndexCalls = new AtomicLong();
            final AtomicLong sumLatencyNanos = new AtomicLong();
            final AtomicLong maxLatencyNanos = new AtomicLong();
            final AtomicLong maxToEndNanos = new AtomicLong();
            final AtomicLong maxReadNanos = new AtomicLong();
            final AtomicLongArray readerIterationStartNanos = new AtomicLongArray(readerCount);
            final Thread[] readerThreads = new Thread[readerCount];
            final AtomicLong stuckEventCount = new AtomicLong();
            final AtomicLong maxStuckMillis = new AtomicLong();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final CyclicBarrier barrier = new CyclicBarrier(writerCount + readerCount);
            final List<Future<?>> futures = new ArrayList<>();

            for (int w = 0; w < writerCount; w++) {
                final int wid = w;
                futures.add(exec.submit(() -> {
                    try (ExcerptAppender ap = queue.createAppender()) {
                        barrier.await();
                        long count = 0;
                        while (!stop.get()) {
                            try (DocumentContext dc = ap.writingDocument()) {
                                Bytes<?> bytes = dc.wire().bytes();
                                bytes.writeInt(wid);
                                bytes.writeLong(count++);
                            }
                            totalWrites.incrementAndGet();
                            if (writerDelayNs > 0)
                                busyWaitNanos(writerDelayNs);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }));
            }
            for (int r = 0; r < readerCount; r++) {
                final int rid = r;
                futures.add(exec.submit(() -> {
                    readerThreads[rid] = Thread.currentThread();
                    // ONE tailer per reader for the entire run -- allocations should scale with
                    // reader count, not iteration count.
                    try (ExcerptTailer tailer = queue.createTailer()
                            .direction(TailerDirection.BACKWARD)) {
                        barrier.await();
                        while (!stop.get()) {
                            long t0 = System.nanoTime();
                            readerIterationStartNanos.lazySet(rid, t0);
                            tailer.toEnd();
                            long t1 = System.nanoTime();
                            try (DocumentContext dc = tailer.readingDocument()) {
                                if (dc.isPresent() && !dc.isMetaData())
                                    dc.index();
                            }
                            long t2 = System.nanoTime();
                            maxToEndNanos.accumulateAndGet(t1 - t0, Math::max);
                            maxReadNanos.accumulateAndGet(t2 - t1, Math::max);
                            long elapsed = t2 - t0;
                            totalLastIndexCalls.incrementAndGet();
                            sumLatencyNanos.addAndGet(elapsed);
                            maxLatencyNanos.accumulateAndGet(elapsed, Math::max);
                            readerIterationStartNanos.lazySet(rid, 0L);
                            if (readerDelayNs > 0)
                                busyWaitNanos(readerDelayNs);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }));
            }

            // Watchdog: every 10ms check per-reader iteration-start. Snapshot the stack of any
            // reader stuck > 50ms. StackTrace.forThread captures the trace as a Throwable so it
            // formats with a familiar exception layout including the thread name.
            Thread watchdog = new Thread(() -> {
                long[] lastReportedStart = new long[readerCount];
                while (!stop.get()) {
                    long now = System.nanoTime();
                    for (int i = 0; i < readerCount; i++) {
                        long start = readerIterationStartNanos.get(i);
                        if (start == 0L)
                            continue;
                        long elapsedMs = (now - start) / 1_000_000L;
                        if (elapsedMs >= WATCHDOG_THRESHOLD_MS && start != lastReportedStart[i]) {
                            Thread t = readerThreads[i];
                            if (t == null)
                                continue;
                            stuckEventCount.incrementAndGet();
                            maxStuckMillis.accumulateAndGet(elapsedMs, Math::max);
                            StackTrace snapshot = StackTrace.forThread(t);
                            System.out.printf("[watchdog] +%dms reader %d stuck for %dms%n",
                                    System.currentTimeMillis() - testStartMillis, i, elapsedMs);
                            snapshot.printStackTrace(System.out);
                            lastReportedStart[i] = start;
                        }
                    }
                    try {
                        Thread.sleep(WATCHDOG_POLL_MS);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }, "scq-fastpath-watchdog");
            watchdog.setDaemon(true);
            watchdog.start();

            long deadline = System.nanoTime() + DURATION_NANOS;
            while (System.nanoTime() < deadline && failure.get() == null) {
                Thread.sleep(500);
            }
            stop.set(true);
            watchdog.interrupt();
            for (GarbageCollectorMXBean bean : gcBeans) {
                try {
                    ((NotificationEmitter) bean).removeNotificationListener(gcListener);
                } catch (Exception ignored) {
                    // best-effort
                }
            }
            for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);

            Path jfrPath = allocRecording.stopAndDump();
            allocRecording.close();
            Jvm.resetExceptionHandlers();

            if (failure.get() != null) {
                System.out.println("FAILURE: " + failure.get());
                failure.get().printStackTrace(System.out);
                System.exit(1);
            }

            long calls = totalLastIndexCalls.get();
            long retries = SCQIndexing.SEQ4POS_FAST_PATH_RETRIES.get() - retriesBefore;
            long bruteForceFallThroughs = SCQIndexing.SEQ4POS_BRUTE_FORCE_FALLTHROUGHS.get() - bruteForceBefore;
            long avgLatencyNs = calls == 0 ? 0 : sumLatencyNanos.get() / calls;
            long maxLatencyNs = maxLatencyNanos.get();
            double retryPerCall = calls == 0 ? 0 : (double) retries / calls;

            long efficientFastPathScans = countCapturedMessages(perfCounts, SCQIndexing.SCAN_LABEL_FAST_PATH);
            long efficientTailChecks = countCapturedMessages(perfCounts, SCQIndexing.SCAN_LABEL_TAIL_CHECK);
            long bruteForceScans = countCapturedMessages(perfCounts, SCQIndexing.SCAN_LABEL_FALL_THROUGH);

            System.out.printf("LastIndexFastPathStress: cores=%d writers=%d readers=%d delayNs=%d writes=%d lastIndexCalls=%d retries=%d retries/call=%.6f avgLatency=%dns maxLatency=%.2fms%n",
                    cores, writerCount, readerCount, writerDelayNs,
                    totalWrites.get(), calls, retries, retryPerCall, avgLatencyNs, maxLatencyNs / 1e6);
            System.out.printf("LastIndexFastPathStress per-phase max: toEnd=%.2fms read=%.2fms%n",
                    maxToEndNanos.get() / 1e6, maxReadNanos.get() / 1e6);
            System.out.printf("LastIndexFastPathStress watchdog: stuckEvents=%d maxStuckMs=%d (threshold=%dms, poll=%dms)%n",
                    stuckEventCount.get(), maxStuckMillis.get(), WATCHDOG_THRESHOLD_MS, WATCHDOG_POLL_MS);
            System.out.printf("LastIndexFastPathStress gc: events=%d totalMs=%d maxPauseMs=%d%n",
                    totalGcEvents.get(), totalGcMillis.get(), maxGcMillis.get());
            System.out.printf("LastIndexFastPathStress slow-scan breakdown: bruteForceFallThroughs=%d efficientFastPath=%d efficientTailCheck=%d bruteForceScans=%d%n",
                    bruteForceFallThroughs, efficientFastPathScans, efficientTailChecks, bruteForceScans);
            if (jfrPath != null)
                System.out.println("LastIndexFastPathStress allocation profile: " + jfrPath +
                        "  (try: jfr print --events 'ObjectAllocation*' " + jfrPath + " | head -200)");
            else
                System.out.println("LastIndexFastPathStress allocation profile: unavailable (jdk.jfr not present)");
        } finally {
            Jvm.resetExceptionHandlers();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static long countCapturedMessages(ConcurrentMap<String, AtomicLong> perfCounts, String fragment) {
        return perfCounts.entrySet().stream()
                .filter(e -> e.getKey().contains(fragment))
                .mapToLong(e -> e.getValue().get())
                .sum();
    }

    /** Tight nanosecond busy-wait. Used for the optional symmetric writer/reader delay knob;
     *  default is 0 so this is never called. */
    private static void busyWaitNanos(long nanos) {
        long deadline = System.nanoTime() + nanos;
        while (System.nanoTime() < deadline) {
            // spin
        }
    }

    private static AllocationRecording startAllocationRecording() {
        try {
            Class<?> recordingClass = Class.forName("jdk.jfr.Recording");
            Object recording = recordingClass.getConstructor().newInstance();
            recordingClass.getMethod("setName", String.class).invoke(recording, "LastIndexFastPath-allocations");
            enableAllocationEvent(recordingClass, recording, "jdk.ObjectAllocationInNewTLAB");
            enableAllocationEvent(recordingClass, recording, "jdk.ObjectAllocationOutsideTLAB");
            recordingClass.getMethod("start").invoke(recording);
            return new JfrAllocationRecording(recordingClass, recording);
        } catch (Exception | LinkageError e) {
            return NoopAllocationRecording.INSTANCE;
        }
    }

    private static void enableAllocationEvent(Class<?> recordingClass, Object recording, String eventName) throws Exception {
        Object eventSettings = recordingClass.getMethod("enable", String.class).invoke(recording, eventName);
        eventSettings.getClass().getMethod("withStackTrace").invoke(eventSettings);
    }

    private interface AllocationRecording {
        Path stopAndDump();

        void close();
    }

    private enum NoopAllocationRecording implements AllocationRecording {
        INSTANCE;

        @Override
        public Path stopAndDump() {
            return null;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static final class JfrAllocationRecording implements AllocationRecording {
        private final Class<?> recordingClass;
        private final Object recording;

        private JfrAllocationRecording(Class<?> recordingClass, Object recording) {
            this.recordingClass = recordingClass;
            this.recording = recording;
        }

        @Override
        public Path stopAndDump() {
            try {
                recordingClass.getMethod("stop").invoke(recording);
                Path jfrPath = Files.createTempFile("LastIndexFastPathStress-", ".jfr");
                recordingClass.getMethod("dump", Path.class).invoke(recording, jfrPath);
                return jfrPath;
            } catch (Exception | LinkageError e) {
                return null;
            }
        }

        @Override
        public void close() {
            try {
                recordingClass.getMethod("close").invoke(recording);
            } catch (Exception | LinkageError ignored) {
                // best-effort
            }
        }
    }
}
