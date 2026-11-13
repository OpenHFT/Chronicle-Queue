/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MoveToCycleMultiThreadedStressTest extends QueueTestCommon {

    private final AtomicLong lastCycle = new AtomicLong();

    // leave a core free for the appender/main thread so the busy-spinning readers can't starve it
    private static final int READ_THREADS = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() - 1, 10));

    private final AtomicBoolean shutDown = new AtomicBoolean();
    private boolean resourceTracing;

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Before
    public void disableResourceTracing() {
        // with this enabled, and a 32 GB heap this fails with flight recorder
        // with this disabled, and a 32 *MB* heap this passes with flight recorder on
        resourceTracing = Jvm.isResourceTracing();
        Jvm.setResourceTracing(false);
    }

    @After
    public void resetResourceTracing() {
        Jvm.setResourceTracing(resourceTracing);
    }

    @Test(timeout = 60000)
    public void test() throws InterruptedException {
        final String path = OS.getTarget() + "/stressMoveToCycle-" + Time.uniqueId();
        final ExecutorService es = Executors.newCachedThreadPool();

        // resources are closed before the finally block runs, so the queue is closed before the pool is shut down
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.binary(path)
                .testBlockSize()
                .rollCycle(net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY)
                .build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {
            try {
                runStress(queue, excerptAppender, es);
            } finally {
                // A reader left running can acquire a store after the queue is closed, leaking it into a later test's
                // resource checks
                stopReaders(es);
            }
        } finally {
            // always stop the pool, otherwise a failure leaves idle threads behind for the thread-leak check
            Threads.shutdown(es);
            IOTools.deleteDirWithFiles(path);
        }
    }

    private void runStress(ChronicleQueue queue, ExcerptAppender excerptAppender, ExecutorService es) throws InterruptedException {
        excerptAppender.writeText("first");
        updateLast(excerptAppender);

        final long firstCycle = excerptAppender.queue().rollCycle().toCycle(queue.firstIndex());

        final Future<Void> appender = es.submit(() -> append(queue));
        final List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < READ_THREADS; i++) {
            futures.add(es.submit(() -> randomMove(queue, firstCycle)));
        }

        AssertionError failure = null;
        try {
            // a healthy run takes ~5s; fail well inside the 60s test timeout if the appender stalls
            appender.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // dump while the appender is still stuck, before cancelling it
            System.err.println(allThreadStacks("appender timeout"));
            appender.cancel(true);
            failure = new AssertionError("Appender did not complete within 30 seconds - possible deadlock or starvation with moveToCycle readers");
        } catch (ExecutionException e) {
            failure = new AssertionError("Appender failed", e.getCause());
        }

        // always stop and wait for the readers before the queue is closed, even if the appender failed.
        // a single moveToCycle can take hundreds of ms under this load, so allow a shared deadline rather than 1s each
        shutDown.set(true);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (Future<Void> future : futures) {
            try {
                future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                if (failure == null) {
                    System.err.println(allThreadStacks("reader timeout"));
                    failure = new AssertionError("Reader did not stop within 10 seconds of shutdown", e);
                }
            } catch (ExecutionException e) {
                if (failure == null)
                    failure = new AssertionError("Reader failed", e.getCause());
            }
        }

        if (failure != null)
            throw failure;
    }

    private void stopReaders(ExecutorService es) throws InterruptedException {
        shutDown.set(true);
        es.shutdownNow();
        if (!es.awaitTermination(10, TimeUnit.SECONDS))
            Jvm.warn().on(getClass(), "Reader threads did not terminate");
    }

    private Void append(ChronicleQueue queue) {

        try (final ExcerptAppender excerptAppender = queue.createAppender()) {

            for (int i = 0; i < 50; i++) {
                excerptAppender.writeText("hello");
                updateLast(excerptAppender);
                Jvm.pause(100);
            }
            return null;
        }
    }

    private static String allThreadStacks(String reason) {
        final StringBuilder sb = new StringBuilder("Thread dump on ").append(reason).append(":\n");
        for (ThreadInfo info : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
            sb.append('"').append(info.getThreadName()).append("\" ").append(info.getThreadState());
            if (info.getLockName() != null)
                sb.append(" on ").append(info.getLockName());
            if (info.getLockOwnerName() != null)
                sb.append(" owned by \"").append(info.getLockOwnerName()).append('"');
            sb.append('\n');
            for (StackTraceElement element : info.getStackTrace())
                sb.append("    at ").append(element).append('\n');
            sb.append('\n');
        }
        return sb.toString();
    }

    private void updateLast(ExcerptAppender excerptAppender) {
        final long lastIndex = excerptAppender.lastIndexAppended();
        lastCycle.set(excerptAppender.queue().rollCycle().toCycle(lastIndex));
    }

    private Void randomMove(ChronicleQueue queue, long firstCycle) {
        try (final ExcerptTailer tailer = queue.createTailer()) {
            while (!shutDown.get()) {
                // pick uniformly from [firstCycle, lastCycle] inclusive, so the current cycle is included
                final int cycle = (int) ThreadLocalRandom.current().nextLong(firstCycle, lastCycle.get() + 1);
                tailer.moveToCycle(cycle);
            }
        }
        return null;
    }
}
