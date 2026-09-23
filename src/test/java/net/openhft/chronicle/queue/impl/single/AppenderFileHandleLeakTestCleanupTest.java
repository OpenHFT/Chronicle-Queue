/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

public class AppenderFileHandleLeakTestCleanupTest extends QueueTestCommon {
    @Test
    public void cleanupWaitsForWorkersWhenInterruptedOnEntry() throws Exception {
        checkInterruptedCleanup(true);
    }

    @Test
    public void cleanupWaitsForWorkersWhenInterruptedDuringTermination() throws Exception {
        checkInterruptedCleanup(false);
    }

    private void checkInterruptedCleanup(boolean interruptOnEntry) throws Exception {
        ObservedExecutor executor = new ObservedExecutor();
        AppenderFileHandleLeakTest fixture = new AppenderFileHandleLeakTest(executor);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch workerSawShutdown = new CountDownLatch(1);
        CountDownLatch allowWorkerToFinish = new CountDownLatch(1);
        Future<?> worker = executor.submit(() -> {
            workerStarted.countDown();
            try {
                allowWorkerToFinish.await();
            } catch (InterruptedException expected) {
                workerSawShutdown.countDown();
                try {
                    // Keep the worker alive while the cleanup thread is interrupted.
                    allowWorkerToFinish.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        FutureTask<Boolean> cleanup = new FutureTask<>(() -> {
            if (interruptOnEntry)
                Thread.currentThread().interrupt();
            fixture.preAfter();
            assertTrue("Cleanup must finish before subsequent checks", executor.isTerminated());
            return Thread.currentThread().isInterrupted();
        });
        Thread cleaner = new Thread(cleanup, "appender-test-cleanup");
        try {
            assertTrue("Worker did not start", workerStarted.await(5, SECONDS));
            cleaner.start();
            assertTrue("Worker did not receive shutdown", workerSawShutdown.await(5, SECONDS));
            Long remaining = executor.waitBudgets.poll(5, SECONDS);
            assertNotNull("Cleanup did not await termination", remaining);
            if (!interruptOnEntry) {
                for (int i = 0; i < 2; i++) {
                    cleaner.interrupt();
                    Long nextRemaining = executor.waitBudgets.poll(5, SECONDS);
                    assertNotNull("Interrupted cleanup must resume waiting", nextRemaining);
                    assertTrue("Interruptions must not restart the timeout", nextRemaining < remaining);
                    assertFalse("Cleanup returned while its worker was still running", cleanup.isDone());
                    remaining = nextRemaining;
                }
            }
            allowWorkerToFinish.countDown();
            assertTrue("Cleanup must restore interrupt status", cleanup.get(5, SECONDS));
            worker.get(5, SECONDS);
        } finally {
            allowWorkerToFinish.countDown();
            executor.shutdownNow();
            cleaner.interrupt();
            cleaner.join(5_000);
            assertFalse("Cleanup thread did not stop", cleaner.isAlive());
            assertTrue("Worker did not stop", executor.awaitTermination(5, SECONDS));
        }
    }

    private static final class ObservedExecutor extends ThreadPoolExecutor {
        private final BlockingQueue<Long> waitBudgets = new LinkedBlockingQueue<>();

        private ObservedExecutor() {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            waitBudgets.add(unit.toNanos(timeout));
            return super.awaitTermination(timeout, unit);
        }
    }
}
