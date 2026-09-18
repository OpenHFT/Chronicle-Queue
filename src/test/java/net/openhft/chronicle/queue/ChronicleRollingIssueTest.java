/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertTrue;

@RequiredForClient
public class ChronicleRollingIssueTest extends QueueTestCommon {

    String path;
    private volatile ExecutorService workers;
    private final CountDownLatch bodyFinished = new CountDownLatch(1);

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
        path = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();
    }

    @Override
    protected void preAfter() {
        if (workers == null)
            return;
        try {
            stopWriters();
            if (bodyFinished.getCount() != 0)
                assertTrue("Rolling test body did not stop", bodyFinished.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while stopping rolling test", e);
        }
    }

    @Override
    protected void tearDown() {
        // A JUnit timeout can start teardown while the test body is still unwinding.
        // Never delete files while either the reader or a writer can still use them.
        assertTrue("Rolling test body still running", workers == null || bodyFinished.getCount() == 0);
        assertTrue("Rolling writers still running", workers == null || workers.isTerminated());
        if (path != null && new File(path).exists())
            assertTrue("Could not delete rolling test files", IOTools.deleteDirWithFiles(path));
        super.tearDown();
    }

    @Test
    public void test() throws Exception {
        int threads = Math.min(64, Runtime.getRuntime().availableProcessors() * 4) - 1;
        int messages = 100;

        StoreFileListener storeFileListener = (cycle, file) -> {
        };
        Runnable appendRunnable = () -> {
            try (final ChronicleQueue writeQueue = ChronicleQueue
                    .singleBuilder(path)
                    .testBlockSize()
                    .storeFileListener(storeFileListener)
                    .rollCycle(TEST_SECONDLY).build();
                 ExcerptAppender appender = writeQueue.createAppender()) {
                for (int i = 0; i < messages; i++) {
                    if (Thread.currentThread().isInterrupted())
                        throw new AssertionError("Rolling writer interrupted");
                    long millis = System.currentTimeMillis() % 100;
                    if (millis > 1 && millis < 99) {
                        Jvm.pause(99 - millis);
                    }
                    Map<String, Object> map = new HashMap<>();
                    map.put("key", Thread.currentThread().getName() + " - " + i);
                    appender.writeMap(map);
                }
            }
        };

        runTest(threads, messages, appendRunnable);
    }

    void runTest(int threads, int messages, Runnable appendRunnable) throws Exception {
        AtomicInteger writerId = new AtomicInteger();
        workers = Executors.newFixedThreadPool(threads,
                task -> new Thread(task, "appender-" + writerId.getAndIncrement()));
        List<Future<?>> results = new ArrayList<>();
        CompletionService<Void> completed = new ExecutorCompletionService<>(workers);
        Throwable failure = null;
        try {
            for (int i = 0; i < threads; i++)
                results.add(completed.submit(appendRunnable, null));
            workers.shutdown();
            readMessages(threads * messages, results);
            // Observe completion order so a blocked close cannot hide a later writer failure.
            for (int i = 0; i < results.size(); i++)
                completed.take().get();
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            try {
                stopWriters();
            } catch (Exception | Error cleanupFailure) {
                if (failure == null)
                    throw cleanupFailure;
                failure.addSuppressed(cleanupFailure);
            } finally {
                bodyFinished.countDown();
            }
        }
    }

    private void readMessages(int expectedMessages, List<Future<?>> results) throws Exception {
        long start = System.currentTimeMillis();
        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(path)
                .testBlockSize()
                .storeFileListener((cycle, file) -> { })
                .rollCycle(TEST_SECONDLY).build();
             ExcerptTailer tailer = queue.createTailer()) {
            int count2 = 0;
            while (count2 < expectedMessages) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException("Rolling reader interrupted");
                // Inspect every completed writer; an earlier blocked writer must not hide a later failure.
                for (Future<?> result : results)
                    if (result.isDone())
                        result.get();
                Map<String, Object> map = tailer.readMap();
                long index = tailer.index();
                if (map != null) {
                    count2++;
                }
                if (System.currentTimeMillis() > start + 60000) {
                    throw new AssertionError("Expected: " + expectedMessages
                            + " read: " + count2
                            + " index: " + Long.toHexString(index));
                }
            }
        }
    }

    private void stopWriters() {
        if (workers == null)
            return;
        boolean interrupted = Thread.interrupted();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try {
            workers.shutdownNow();
            while (!workers.isTerminated()) {
                try {
                    long remaining = deadline - System.nanoTime();
                    assertTrue("Rolling writers did not stop", remaining > 0);
                    assertTrue("Rolling writers did not stop", workers.awaitTermination(remaining, TimeUnit.NANOSECONDS));
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
