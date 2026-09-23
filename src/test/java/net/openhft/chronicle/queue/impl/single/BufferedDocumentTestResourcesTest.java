/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class BufferedDocumentTestResourcesTest {
    @Test
    @SuppressWarnings("try")
    public void failedJoinRetainsStorageAndTheOriginalFailure() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        BufferedDocumentTestResources resources = new BufferedDocumentTestResources(worker, 100, TimeUnit.MILLISECONDS);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean closed = new AtomicBoolean();
        resources.own(() -> {
            assertTrue("Worker must exit before storage closes", worker.isTerminated());
            closed.set(true);
        });
        worker.submit(() -> {
            started.countDown();
            while (release.getCount() != 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    // Deliberately refuse termination until the controlling thread releases us.
                }
            }
        });
        try {
            assertTrue(started.await(5, TimeUnit.SECONDS));
            AssertionError body = new AssertionError("original body failure");
            Throwable actual = assertThrows(AssertionError.class, () -> {
                try (BufferedDocumentTestResources ignored = resources) {
                    throw body;
                }
            });
            assertSame(body, actual);
            assertEquals(1, body.getSuppressed().length);
            assertTrue(body.getSuppressed()[0].getMessage().contains("storage retained"));
            assertFalse(closed.get());
        } finally {
            release.countDown();
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            resources.close();
        }
        assertTrue(closed.get());
    }

    @Test
    public void interruptedCleanupJoinsBeforeClosingAndRestoresInterrupt() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        BufferedDocumentTestResources resources = new BufferedDocumentTestResources(worker);
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean closed = new AtomicBoolean();
        resources.own(() -> {
            assertTrue(worker.isTerminated());
            closed.set(true);
        });
        worker.submit(() -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            assertTrue(started.await(5, TimeUnit.SECONDS));
            Thread.currentThread().interrupt();
            resources.close();
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(closed.get());
        } finally {
            Thread.interrupted();
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            resources.close();
        }
    }
}
