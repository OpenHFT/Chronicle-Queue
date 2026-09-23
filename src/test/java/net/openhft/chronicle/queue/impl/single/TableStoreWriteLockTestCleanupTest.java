/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

public class TableStoreWriteLockTestCleanupTest {
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
            TableStoreWriteLockTest.stopProcess(process);
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
                    () -> TableStoreWriteLockTest.stopProcess(process, 100, TimeUnit.MILLISECONDS));
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
