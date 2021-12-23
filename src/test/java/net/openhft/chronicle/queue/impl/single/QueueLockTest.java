package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueTestBase;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class QueueLockTest extends ChronicleQueueTestBase {

    @Test
    public void testTimeout() throws InterruptedException {
        expectException("queue.dont.recover.lock.timeout property is deprecated and will be removed");
        check(true);
    }

    @Test
    public void testRecover() throws InterruptedException {
        try {
            check(false);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("overwritten? Expected:"));
        }
    }

    private void check(boolean shouldThrowException) throws InterruptedException {
        finishedNormally = false;
        ignoreException("Couldn't acquire write lock");
        if (!shouldThrowException)
            expectException("Forced unlock for the lock");

        try {
            System.setProperty("queue.dont.recover.lock.timeout", Boolean.toString(shouldThrowException));

            final long timeoutMs = 2_000;
            final File queueDir = DirectoryUtils.tempDir("check");
            try (final RollingChronicleQueue queue = ChronicleQueue.singleBuilder(queueDir).
                    timeoutMS(timeoutMs).
                    build()) {

                // lock the queue
                try (DocumentContext dc = queue.acquireAppender().writingDocument()) {

                    final CountDownLatch started = new CountDownLatch(1);
                    final CountDownLatch finished = new CountDownLatch(1);
                    final AtomicBoolean recoveredAndAcquiredTheLock = new AtomicBoolean();
                    final AtomicBoolean threwException = new AtomicBoolean();

                    final Thread otherWriter = new Thread(() -> {
                        try (final RollingChronicleQueue queue2 = ChronicleQueue.singleBuilder(queueDir).
                                timeoutMS(timeoutMs).
                                build()) {
                            started.countDown();
                            try (DocumentContext ignored = queue2.acquireAppender().writingDocument()) {
                                recoveredAndAcquiredTheLock.set(true);
                                System.out.println("Done");
                            } catch (UnrecoverableTimeoutException e) {
                                e.printStackTrace();
                                threwException.set(true);
                            } catch (Throwable t) {
                                t.printStackTrace();
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
            System.clearProperty("queue.dont.recover.lock.timeout");
        }
    }
}
