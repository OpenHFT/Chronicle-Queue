/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class TableStoreWriteLockTestResourcesTest {
    @Test(timeout = 10_000)
    public void waitsForDelayedForcedExitWithAnInterruptedCaller() throws Exception {
        checkInterruptedCleanup(true);
    }

    @Test(timeout = 10_000)
    public void repeatedInterruptionsDoNotAbandonPendingTermination() throws Exception {
        checkInterruptedCleanup(false);
    }

    private void checkInterruptedCleanup(boolean interruptOnEntry) throws Exception {
        ControlledProcess process = new ControlledProcess();
        FutureTask<Boolean> cleanup = new FutureTask<>(() -> {
            if (interruptOnEntry)
                Thread.currentThread().interrupt();
            TableStoreWriteLockTestResources.stopProcess(process, 2, SECONDS);
            assertFalse("Cleanup returned before process termination", process.isAlive());
            return Thread.currentThread().isInterrupted();
        });
        Thread cleaner = new Thread(cleanup, "table-store-process-cleanup");
        try {
            cleaner.start();
            Long remaining = process.waitBudgets.poll(5, SECONDS);
            assertNotNull("Cleanup did not await termination", remaining);
            if (!interruptOnEntry) {
                cleaner.interrupt();
                remaining = process.waitBudgets.poll(5, SECONDS);
                assertNotNull("Cleanup did not wait after its first interruption", remaining);
            }
            assertTrue("Cleanup did not request forced termination", process.forced.await(5, SECONDS));
            assertFalse("A destruction request must not count as termination", cleanup.isDone());
            for (int i = 0; i < 2; i++) {
                cleaner.interrupt();
                Long next = process.waitBudgets.poll(5, SECONDS);
                assertNotNull("Repeated interruption abandoned termination", next);
                assertTrue("Interruptions must not restart the deadline", next < remaining);
                assertFalse("Cleanup returned while termination was pending", cleanup.isDone());
                remaining = next;
            }
            process.exited.countDown();
            assertTrue("Cleanup must restore the caller's interrupt flag", cleanup.get(5, SECONDS));
        } finally {
            process.exited.countDown();
            cleaner.interrupt();
            cleaner.join(5_000);
            assertFalse("Cleanup thread did not stop", cleaner.isAlive());
        }
    }

    @Test(timeout = 5_000)
    public void reportsDeadlineExhaustionAndRestoresInterruption() {
        ControlledProcess process = new ControlledProcess();
        Thread.currentThread().interrupt();
        try {
            long start = System.nanoTime();
            AssertionError error = assertThrows(AssertionError.class,
                    () -> TableStoreWriteLockTestResources.stopProcess(process, 100, TimeUnit.MILLISECONDS));
            assertTrue(error.getMessage().contains("remains alive"));
            assertTrue("The termination budget was not used", System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(100));
            assertTrue(process.isAlive());
            assertEquals(0, process.forced.getCount());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            process.exited.countDown();
        }
    }


    @Test(timeout = 5_000)
    public void closesExecutorAfterFailureWithInterruptedCaller() throws Exception {
        TableStoreWriteLockTestResources resources = new TableStoreWriteLockTestResources();
        ExecutorService executor = resources.ownExecutor(Executors.newSingleThreadExecutor());
        CountDownLatch started = new CountDownLatch(1);
        Future<?> worker = executor.submit(() -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            assertTrue(started.await(2, SECONDS));
            Thread.currentThread().interrupt();
            resources.close();
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue("Fixture must await executor termination", executor.isTerminated());
            assertTrue(worker.isDone());
        } finally {
            Thread.interrupted();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2, SECONDS));
        }
    }

    @Test
    public void continuesCleanupAfterAnEarlierFailure() {
        TableStoreWriteLockTestResources resources = new TableStoreWriteLockTestResources();
        java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
        AssertionError original = new AssertionError("Deliberate cleanup failure");
        resources.own(() -> closed.set(true), "remaining owner");
        resources.own(() -> { throw original; }, "failing owner");
        AssertionError reported = assertThrows(AssertionError.class, resources::close);
        assertSame(original, reported.getCause());
        assertTrue(closed.get());
        assertTrue(resources.diagnostics().contains("failing owner"));
        resources.close();
    }

    @Test
    public void releasesAnOwnerRegisteredAfterTeardown() {
        TableStoreWriteLockTestResources resources = new TableStoreWriteLockTestResources();
        java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
        resources.close();
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> resources.own(() -> closed.set(true), "late owner"));
        assertTrue(closed.get());
        assertTrue(failure.getMessage().contains("late owner"));
    }

    @Test
    public void preservesAnInterruptRaisedDuringCleanup() {
        TableStoreWriteLockTestResources resources = new TableStoreWriteLockTestResources();
        resources.own(() -> Thread.interrupted(), "consuming owner");
        resources.own(() -> Thread.currentThread().interrupt(), "interrupting owner");
        try {
            resources.close();
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test(timeout = 5_000)
    public void reportsAStillMappedFileInsteadOfIgnoringDeletionFailure() throws Exception {
        assumeTrue(OS.isWindows());
        Path directory = IOTools.createTempDirectory("lockCleanupControl");
        try {
            try (MappedBytes bytes = MappedBytes.mappedBytes(directory.resolve("held.cq4t").toFile(), 64 << 10)) {
                bytes.writeLong(42);
                AssertionError failure = assertThrows(AssertionError.class,
                        () -> TableStoreWriteLockTestResources.deleteDirectory(directory, 100, TimeUnit.MILLISECONDS));
                assertTrue(failure.getMessage(), failure.getMessage().contains("held.cq4t"));
                assertTrue(directory.toFile().exists());
                assertEquals(42, bytes.readLong(0));
            }
        } finally {
            TableStoreWriteLockTestResources.deleteDirectory(directory, 2, SECONDS);
        }
    }

    // Separate the OS destruction request from exit, using real interruptible waits.
    private static final class ControlledProcess extends Process {
        private final CountDownLatch forced = new CountDownLatch(1);
        private final CountDownLatch exited = new CountDownLatch(1);
        private final BlockingQueue<Long> waitBudgets = new LinkedBlockingQueue<>();

        @Override
        public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
            waitBudgets.add(unit.toNanos(timeout));
            return exited.await(timeout, unit);
        }

        @Override
        public int waitFor() throws InterruptedException {
            exited.await();
            return 0;
        }

        @Override
        public boolean isAlive() {
            return exited.getCount() != 0;
        }

        @Override
        public int exitValue() {
            if (isAlive())
                throw new IllegalThreadStateException("Termination is still pending");
            return 0;
        }

        @Override
        public void destroy() {
            // A normal destruction request is deliberately ignored.
        }

        @Override
        public Process destroyForcibly() {
            forced.countDown();
            return this;
        }

        @Override
        public OutputStream getOutputStream() {
            return new ByteArrayOutputStream();
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public InputStream getErrorStream() {
            return new ByteArrayInputStream(new byte[0]);
        }
    }
}
