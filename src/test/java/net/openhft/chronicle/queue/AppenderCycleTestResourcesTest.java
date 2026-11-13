/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AppenderCycleTestResourcesTest extends QueueTestCommon {
    @Test
    public void earlyBodyFailureJoinsBothWorkersBeforeReleasingStorage() {
        AppenderCycleTestResources resources = new AppenderCycleTestResources("cycle-early-failure");
        CountDownLatch started = new CountDownLatch(2);
        Thread[] workers = new Thread[2];
        AssertionError original = new AssertionError("Deliberate failure before go");
        AssertionError actual = assertThrows(AssertionError.class, () -> {
            try (AppenderCycleTestResources owned = resources) {
                for (int i = 0; i < workers.length; i++)
                    workers[i] = owned.start("cycle-waiting-" + i, appender -> {
                        started.countDown();
                        owned.await(owned.go, "go");
                    });
                owned.await(started, "started");
                throw original;
            }
        });
        assertSame(original, actual);
        assertEquals("Cleanup should not add a secondary failure", 0, actual.getSuppressed().length);
        for (Thread worker : workers)
            assertFalse(worker.isAlive());
        assertEquals(0, resources.message.refCount());
        assertFalse(resources.directory.toFile().exists());
    }

    @Test
    public void workerFailureIsReportedBeforeTheReadyDeadline() {
        AppenderCycleTestResources resources = new AppenderCycleTestResources("cycle-worker-failure");
        AssertionError original = new AssertionError("Deliberate worker failure before ready");
        Thread[] workers = new Thread[2];
        AssertionError actual = assertThrows(AssertionError.class, () -> {
            try (AppenderCycleTestResources owned = resources) {
                workers[0] = owned.start("cycle-failing", appender -> { throw original; });
                workers[1] = owned.start("cycle-waiting", appender -> owned.await(owned.go, "go"));
                owned.await(new CountDownLatch(1), "unreachable ready");
            }
        });
        assertSame(original, actual.getCause());
        for (Thread worker : workers)
            assertFalse(worker.isAlive());
        assertEquals(0, resources.message.refCount());
        assertFalse(resources.directory.toFile().exists());
    }

    @Test
    public void interruptedCleanupWaitsForDelayedWorkersBeforeFreeingStorage() throws Exception {
        AppenderCycleTestResources resources = new AppenderCycleTestResources("cycle-delayed-cleanup");
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch interrupted = new CountDownLatch(2);
        CountDownLatch allowExit = new CountDownLatch(1);
        Thread[] workers = new Thread[2];
        FutureTask<Boolean> cleanup = new FutureTask<>(() -> {
            Thread.currentThread().interrupt();
            resources.close();
            return Thread.currentThread().isInterrupted();
        });
        Thread cleaner = new Thread(cleanup, "cycle-cleaner");
        try {
            for (int i = 0; i < workers.length; i++)
                workers[i] = resources.start("cycle-delayed-" + i, appender -> {
                    started.countDown();
                    try {
                        new CountDownLatch(1).await();
                    } catch (InterruptedException expected) {
                        interrupted.countDown();
                        try {
                            assertTrue("Control did not release delayed cleanup", allowExit.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException repeated) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError("Cleanup interrupted the worker repeatedly", repeated);
                        }
                    }
                });
            resources.await(started, "started");
            cleaner.start();
            assertTrue("Cleanup must interrupt both workers before joining", interrupted.await(5, TimeUnit.SECONDS));
            assertFalse("Cleanup returned before the workers exited", cleanup.isDone());
            assertTrue("Shared bytes were freed while still owned", resources.message.refCount() > 0);
            assertTrue("Queue storage was deleted while still owned", resources.directory.toFile().exists());
            allowExit.countDown();
            assertTrue("Cleanup must preserve caller interruption", cleanup.get(5, TimeUnit.SECONDS));
            for (Thread worker : workers)
                assertFalse(worker.isAlive());
            assertEquals(0, resources.message.refCount());
            assertFalse(resources.directory.toFile().exists());
        } finally {
            allowExit.countDown();
            cleaner.join(5_000);
            assertFalse("Cleanup control must terminate", cleaner.isAlive());
            resources.close();
        }
    }
}
