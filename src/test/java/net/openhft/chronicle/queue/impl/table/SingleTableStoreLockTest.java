/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.testframework.ExecutorServiceUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SingleTableStoreLockTest extends QueueTestCommon {

    private static final int LOCK_SIZE = 1;
    private static final long LOCK_START = Long.MAX_VALUE - LOCK_SIZE;

    @Before
    public void trackTestThreads() {
        threadDump();
    }

    @Test
    public void exclusiveBodyRunsOnceAndPreservesRuntimeExceptionAndErrorIdentity() throws IOException {
        assertBodyRunsOnceAndPreservesFailureIdentity(false);
    }

    @Test
    public void sharedBodyRunsOnceAndPreservesRuntimeExceptionAndErrorIdentity() throws IOException {
        assertBodyRunsOnceAndPreservesFailureIdentity(true);
    }

    @Test
    public void exclusiveContentionIsRetriedBeforeBodyRunsOnce() throws Exception {
        assertContentionIsRetriedBeforeBodyRunsOnce(false);
    }

    @Test
    public void sharedContentionIsRetriedBeforeBodyRunsOnce() throws Exception {
        assertContentionIsRetriedBeforeBodyRunsOnce(true);
    }

    private void assertBodyRunsOnceAndPreservesFailureIdentity(boolean shared) throws IOException {
        final File file = newLockFile();
        final AtomicInteger targetCalls = new AtomicInteger();
        final AtomicInteger bodyCalls = new AtomicInteger();
        final OverlappingFileLockException expectedRuntimeException = new OverlappingFileLockException();

        final OverlappingFileLockException actualRuntimeException = assertThrows(
                OverlappingFileLockException.class,
                () -> withLock(file, shared, target -> {
                    bodyCalls.incrementAndGet();
                    throw expectedRuntimeException;
                }, () -> {
                    targetCalls.incrementAndGet();
                    return "target";
                }));

        assertSame(expectedRuntimeException, actualRuntimeException);
        assertEquals(1, targetCalls.get());
        assertEquals(1, bodyCalls.get());

        targetCalls.set(0);
        bodyCalls.set(0);
        final AssertionError expectedError = new AssertionError("deliberate body failure");

        final AssertionError actualError = assertThrows(AssertionError.class,
                () -> withLock(file, shared, target -> {
                    bodyCalls.incrementAndGet();
                    throw expectedError;
                }, () -> {
                    targetCalls.incrementAndGet();
                    return "target";
                }));

        assertSame(expectedError, actualError);
        assertEquals(1, targetCalls.get());
        assertEquals(1, bodyCalls.get());
    }

    private void assertContentionIsRetriedBeforeBodyRunsOnce(boolean shared) throws Exception {
        final File file = newLockFile();
        final AtomicInteger targetCalls = new AtomicInteger();
        final AtomicInteger bodyCalls = new AtomicInteger();
        final CountDownLatch workerStarted = new CountDownLatch(1);
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        try (FileChannel blockerChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
            final FileLock blocker = blockerChannel.lock(LOCK_START, LOCK_SIZE, false);
            try {
                final Future<String> result = executor.submit(() -> {
                    workerStarted.countDown();
                    return withLock(file, shared, target -> {
                        bodyCalls.incrementAndGet();
                        return target + "-done";
                    }, () -> {
                        targetCalls.incrementAndGet();
                        return "target";
                    });
                });

                assertTrue("lock worker did not start", workerStarted.await(5, TimeUnit.SECONDS));
                assertThrows("body ran while the conflicting lock was still held", TimeoutException.class,
                        () -> result.get(100, TimeUnit.MILLISECONDS));
                assertEquals(0, targetCalls.get());
                assertEquals(0, bodyCalls.get());

                blocker.release();

                assertEquals("target-done", result.get(5, TimeUnit.SECONDS));
                assertEquals(1, targetCalls.get());
                assertEquals(1, bodyCalls.get());
            } finally {
                if (blocker.isValid())
                    blocker.release();
            }
        } finally {
            ExecutorServiceUtil.shutdownForciblyAndWaitForTermination(executor);
        }
    }

    private File newLockFile() throws IOException {
        final File directory = getTmpDir();
        assertTrue(directory.mkdirs() || directory.isDirectory());
        return Files.createFile(new File(directory, "table-lock" + SingleTableStore.SUFFIX).toPath()).toFile();
    }

    private <T, R> R withLock(File file,
                              boolean shared,
                              Function<T, ? extends R> code,
                              Supplier<T> target) {
        if (shared)
            return SingleTableStore.doWithSharedLock(file, code, target);
        return SingleTableStore.doWithExclusiveLock(file, code, target);
    }
}
