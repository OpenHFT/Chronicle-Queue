/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class QueueLockTest extends QueueTestCommon {

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void testTimeout() throws InterruptedException {
        check(true);
    }

    @Test
    public void testRecover() throws InterruptedException {
        System.setProperty("queue.force.unlock.mode", "ALWAYS");
        try {
            check(false);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("overwritten? Expected:"));
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
    }

    private void check(boolean shouldThrowException) throws InterruptedException {
        finishedNormally = false;
        ignoreException("Couldn't acquire write lock");
        if (!shouldThrowException)
            expectException("Forced unlock for the lock");

        try {
            System.setProperty("queue.force.unlock.mode", shouldThrowException ? "NEVER" : "ALWAYS" );

            final long timeoutMs = 2_000;
            final File queueDir = DirectoryUtils.tempDir("check");
            try (final RollingChronicleQueue queue = ChronicleQueue.singleBuilder(queueDir).
                    timeoutMS(timeoutMs).
                    build();
                 final ExcerptAppender excerptAppender = queue.createAppender()) {

                // lock the queue
                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    assertNotNull(dc);
                    final CountDownLatch started = new CountDownLatch(1);
                    final CountDownLatch finished = new CountDownLatch(1);
                    final AtomicBoolean recoveredAndAcquiredTheLock = new AtomicBoolean();
                    final AtomicBoolean threwException = new AtomicBoolean();

                    final Thread otherWriter = new Thread(() -> {
                        try (final RollingChronicleQueue queue2 = ChronicleQueue.singleBuilder(queueDir).
                                timeoutMS(timeoutMs).
                                build()) {
                            started.countDown();
                            try (final ExcerptAppender queue2Appender = queue2.createAppender();
                                 final DocumentContext context = queue2Appender.writingDocument()) {
                                assertNotNull(context);

                                recoveredAndAcquiredTheLock.set(true);
                                System.out.println("Done");
                            } catch (UnrecoverableTimeoutException e) {
                                e.printStackTrace();
                                threwException.set(true);
                            } catch (RuntimeException e) {
                                e.printStackTrace();
                            } finally {
                                System.out.println("finished");
                                finished.countDown();
                            }
                        }
                    }, "Test thread");

                    otherWriter.start();
                    long startTime = System.currentTimeMillis();
                    started.await(1, TimeUnit.SECONDS);
                    finished.await(10, TimeUnit.SECONDS);
                    long endTime = System.currentTimeMillis();
                    long time = endTime - startTime;
                    assertEquals(shouldThrowException, threwException.get());
                    assertEquals(shouldThrowException, !recoveredAndAcquiredTheLock.get());
                    assertTrue("timeout, time: " + time, time >= timeoutMs);
                }
            }
            finishedNormally = true;
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
    }
}
