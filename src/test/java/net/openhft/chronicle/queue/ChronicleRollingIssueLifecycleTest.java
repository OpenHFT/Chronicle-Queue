/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.*;

public class ChronicleRollingIssueLifecycleTest {
    @Test
    public void reportsLaterWriterFailureAndStopsBlockedWriter() throws InterruptedException {
        Result result;
        try {
            result = JUnitCore.runClasses(BlockedWriterFixture.class);
        } finally {
            // Also clean up the negative control, whose reader ignores the writer failure.
            BlockedWriterFixture.release.countDown();
            BlockedWriterFixture.body.join(5_000);
            assertFalse("Controlled test body must terminate", BlockedWriterFixture.body.isAlive());
        }
        assertWriterFailure(result, "later writer failed");
        assertEquals("Blocked writer must close before fixture teardown", 0,
                BlockedWriterFixture.closed.getCount());
    }

    @Test
    public void reportsWriterFailureAfterAllMessagesWerePublished() {
        assertWriterFailure(JUnitCore.runClasses(PublishedWriterFixture.class), "writer close failed");
    }

    private static void assertWriterFailure(Result result, String message) {
        assertEquals(result.getFailures().toString(), 1, result.getFailureCount());
        Throwable failure = result.getFailures().get(0).getException();
        while (failure.getCause() != null)
            failure = failure.getCause();
        assertEquals(result.getFailures().toString(), message, failure.getMessage());
    }

    public static class BlockedWriterFixture extends ChronicleRollingIssueTest {
        private static CountDownLatch closed;
        private static CountDownLatch release;
        private static Thread body;

        public BlockedWriterFixture() {
            globalTimeout = Timeout.seconds(3);
            closed = new CountDownLatch(1);
            release = new CountDownLatch(1);
        }

        @Override
        @Test
        public void test() throws Exception {
            body = Thread.currentThread();
            AtomicInteger writer = new AtomicInteger();
            CountDownLatch opened = new CountDownLatch(1);
            runTest(2, 1, () -> {
                if (writer.getAndIncrement() == 0) {
                    try (ChronicleQueue queue = ChronicleQueue.singleBuilder(path)
                            .testBlockSize().rollCycle(TEST_SECONDLY).build();
                         ExcerptAppender appender = queue.createAppender()) {
                        appender.writeMap(Collections.singletonMap("key", "first"));
                        opened.countDown();
                        release.await();
                        appender.writeMap(Collections.singletonMap("key", "control cleanup"));
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
                    throw new AssertionError("later writer failed");
                }
            });
        }
    }

    public static class PublishedWriterFixture extends ChronicleRollingIssueTest {
        public PublishedWriterFixture() {
            globalTimeout = Timeout.seconds(3);
        }

        @Override
        @Test
        public void test() throws Exception {
            runTest(1, 1, () -> {
                try (ChronicleQueue queue = ChronicleQueue.singleBuilder(path)
                        .testBlockSize().rollCycle(TEST_SECONDLY).build();
                     ExcerptAppender appender = queue.createAppender()) {
                    appender.writeMap(Collections.singletonMap("key", "last"));
                }
                throw new AssertionError("writer close failed");
            });
        }
    }
}
