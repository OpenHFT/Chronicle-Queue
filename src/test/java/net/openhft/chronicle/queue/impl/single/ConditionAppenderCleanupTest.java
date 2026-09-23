/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class ConditionAppenderCleanupTest {
    @Test
    public void initiallyInterruptedCallerStillObservesWorkerExit() throws Exception {
        checkInterruptedJoin(false);
    }

    @Test
    public void repeatedInterruptsDoNotAbandonWorkerExit() throws Exception {
        checkInterruptedJoin(true);
    }

    private void checkInterruptedJoin(boolean repeatedly) throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean closed = new AtomicBoolean();
        Thread caller = Thread.currentThread();
        Thread worker = new Thread(() -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
                closing.countDown();
                try {
                    release.await();
                    closed.set(true);
                } catch (InterruptedException failure) {
                    throw new AssertionError(failure);
                }
            }
        }, "condition-cleanup-control");
        Thread interrupter = new Thread(() -> {
            try {
                if (!closing.await(1, TimeUnit.SECONDS))
                    return;
                for (int i = 0; i < 5; i++) {
                    if (repeatedly)
                        caller.interrupt();
                    Thread.sleep(10);
                }
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
            } finally {
                release.countDown();
            }
        }, "condition-cleanup-interrupter");
        worker.start();
        try {
            assertTrue(started.await(1, TimeUnit.SECONDS));
            interrupter.start();
            caller.interrupt();
            SingleChronicleQueueTest.finishConditionWorker(worker, false);
            assertTrue("Restore the caller's interruption", caller.isInterrupted());
            assertTrue("Observe completed worker cleanup before closing its queue", closed.get());
            assertFalse(worker.isAlive());
        } finally {
            Thread.interrupted();
            release.countDown();
            worker.interrupt();
            interrupter.interrupt();
            worker.join(2000);
            interrupter.join(2000);
        }
    }
}
