/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class MethodWriterWorkerLifecycleTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test(timeout = 15_000)
    public void reportsLaterFailureAndClosesBlockedWriter() throws InterruptedException {
        Result result;
        long closedBeforeControlCleanup;
        try {
            result = JUnitCore.runClasses(BlockedWriterFixture.class);
            closedBeforeControlCleanup = BlockedWriterFixture.closed.getCount();
        } finally {
            // Release the deliberately blocked worker even when testing the broken fixture.
            BlockedWriterFixture.release.countDown();
            assertTrue("Controlled worker did not close", BlockedWriterFixture.closed.await(5, TimeUnit.SECONDS));
            BlockedWriterFixture.body.join(5_000);
            assertFalse("Controlled body did not stop", BlockedWriterFixture.body.isAlive());
        }
        assertEquals(result.getFailures().toString(), 1, result.getFailureCount());
        Throwable failure = result.getFailures().get(0).getException();
        while (failure.getCause() != null)
            failure = failure.getCause();
        assertEquals(result.getFailures().toString(), "later method writer failed", failure.getMessage());
        assertEquals("Worker must close before fixture teardown", 0, closedBeforeControlCleanup);
    }

    @Test(timeout = 15_000)
    public void interruptionStopsWorkersBeforeReturningAndPreservesInterrupt() throws Exception {
        File path = temporaryFolder.newFolder();
        CountDownLatch opened = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(2);
        AtomicBoolean interrupted = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        TestMethodWriterWithThreads fixture = new TestMethodWriterWithThreads(false);
        Thread body = new Thread(() -> {
            try {
                fixture.runTasks(2, index -> {
                    File workerPath = new File(path, "worker-" + index);
                    try (ChronicleQueue queue = ChronicleQueue.singleBuilder(workerPath).testBlockSize().build();
                         ExcerptAppender appender = queue.createAppender()) {
                        appender.writeText("held until the test is interrupted");
                        opened.countDown();
                        release.await();
                    } catch (InterruptedException expectedOnShutdown) {
                        Thread.currentThread().interrupt();
                    } finally {
                        closed.countDown();
                    }
                });
            } catch (Throwable e) {
                failure.set(e);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        }, "controlled-method-writer-body");
        boolean stopped;
        long closedBeforeControlCleanup;
        body.start();
        try {
            assertTrue("Controlled writers did not open", opened.await(5, TimeUnit.SECONDS));
            body.interrupt();
            body.join(2_000);
            stopped = !body.isAlive();
            closedBeforeControlCleanup = closed.getCount();
        } finally {
            release.countDown();
            body.join(5_000);
            assertFalse("Controlled body did not stop", body.isAlive());
            assertTrue("Controlled writers did not close", closed.await(5, TimeUnit.SECONDS));
        }
        assertTrue("Interrupted body must return promptly", stopped);
        assertEquals("Workers must close before interrupted body returns", 0, closedBeforeControlCleanup);
        assertTrue("Original interruption must be retained: " + failure.get(), failure.get() instanceof InterruptedException);
        assertTrue("Cleanup must preserve the interrupt flag", interrupted.get());
    }

    @RunWith(BlockJUnit4ClassRunner.class)
    public static class BlockedWriterFixture extends TestMethodWriterWithThreads {
        private static CountDownLatch release;
        private static CountDownLatch closed;
        private static Thread body;

        public BlockedWriterFixture() {
            super(false);
            globalTimeout = Timeout.seconds(3);
            release = new CountDownLatch(1);
            closed = new CountDownLatch(1);
        }

        @Override
        @Before
        public void check64bit() {
            // This control holds one small Queue; the production stress case keeps its 64-bit guard.
        }

        @Override
        @Test
        public void test() throws Exception {
            body = Thread.currentThread();
            File path = getTmpDir();
            CountDownLatch opened = new CountDownLatch(1);
            runTasks(2, index -> {
                if (index == 0) {
                    try (ChronicleQueue queue = ChronicleQueue.singleBuilder(path).testBlockSize().build();
                         ExcerptAppender appender = queue.createAppender()) {
                        appender.writeText("first writer is still active");
                        opened.countDown();
                        release.await();
                    } catch (InterruptedException expectedOnShutdown) {
                        Thread.currentThread().interrupt();
                    } finally {
                        closed.countDown();
                    }
                } else {
                    try {
                        assertTrue("First writer did not open", opened.await(2, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    throw new AssertionError("later method writer failed");
                }
            });
        }
    }
}
