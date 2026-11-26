/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Owns the cycle-test workers and the storage they can still access during cleanup. */
final class AppenderCycleTestResources implements AutoCloseable {
    final Path directory;
    final Bytes<?> message = Bytes.allocateDirect(64);
    final CountDownLatch go = new CountDownLatch(1);
    private final CountDownLatch done = new CountDownLatch(2);
    private final List<Thread> workers = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean stopping;
    private boolean closed;

    AppenderCycleTestResources(String id) {
        directory = IOTools.createTempDirectory(id);
    }

    Thread start(String name, Consumer<ExcerptAppender> action) {
        Thread worker = new Thread(() -> {
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder
                    .builder(directory, WireType.FIELDLESS_BINARY)
                    .testBlockSize().rollCycle(RollCycles.DEFAULT).build();
                 ExcerptAppender appender = queue.createAppender()) {
                action.accept(appender);
            } catch (Throwable thrown) {
                if (!stopping || !(thrown instanceof InterruptedRuntimeException))
                    failure.compareAndSet(null, thrown);
            } finally {
                done.countDown();
            }
        }, name);
        // A failed bounded shutdown is reported, but must not hang the test JVM forever.
        worker.setDaemon(true);
        workers.add(worker);
        worker.start();
        return worker;
    }

    void await(CountDownLatch latch, String name) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try {
            while (!latch.await(10, TimeUnit.MILLISECONDS)) {
                checkFailure();
                assertTrue("Timed out waiting for " + name, System.nanoTime() < deadline);
            }
            checkFailure();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new InterruptedRuntimeException(interrupted);
        }
    }

    void awaitDone() {
        await(done, "done");
    }

    private void checkFailure() {
        Throwable thrown = failure.get();
        if (thrown != null)
            throw new AssertionError("Appender cycle worker failed", thrown);
    }

    @Override
    public void close() {
        if (closed)
            return;
        stopping = true;
        go.countDown();
        workers.forEach(Thread::interrupt);
        joinWorkers();
        // If any worker survives the deadline, retain its storage instead of freeing live memory.
        closed = true;
        try {
            message.releaseLast();
        } finally {
            BackgroundResourceReleaser.releasePendingResources();
            IOTools.deleteDirWithFilesOrThrow(directory.toFile());
        }
        checkFailure();
    }

    private void joinWorkers() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        boolean interrupted = Thread.interrupted();
        try {
            for (Thread worker : workers) {
                while (worker.isAlive() && System.nanoTime() < deadline) {
                    try {
                        TimeUnit.NANOSECONDS.timedJoin(worker, Math.max(1, deadline - System.nanoTime()));
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
            }
            for (Thread worker : workers)
                assertFalse("Appender cycle worker remains alive: " + worker + ", state=" + worker.getState(),
                        worker.isAlive());
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }
}
