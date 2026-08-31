/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SingleTableStoreSharedLockTimeoutTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    // Zero is a non-blocking policy, not a request to skip the first lock attempt.
    @Test
    public void callerCanSupplySharedLockTimeout() throws Exception {
        File file = temporaryFolder.newFile("metadata.cq4t");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.setLength(1);
        }

        String value = SingleTableStore.doWithSharedLock(file, 0,
                ignored -> "locked", () -> null);

        assertEquals("locked", value);
    }

    // Negative durations are invalid policy input and must not be reported as contention.
    @Test(expected = IllegalArgumentException.class)
    public void negativeSharedLockTimeoutIsRejected() throws Exception {
        File file = temporaryFolder.newFile("metadata.cq4t");
        SingleTableStore.doWithSharedLock(file, -1,
                ignored -> "unreachable", () -> null);
    }

    // A contended zero-timeout call performs exactly one attempt and returns promptly.
    @Test
    public void zeroTimeoutPerformsOneAttemptWhenTheStructuralLockIsHeld() throws Exception {
        File file = temporaryFolder.newFile("locked-metadata.cq4t");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.setLength(1);
            try (FileLock lock = randomAccessFile.getChannel()
                    .lock(Long.MAX_VALUE - 1, 1, false)) {
                assertTrue(lock.isValid());
                try {
                    SingleTableStore.doWithSharedLock(file, 0,
                            value -> "unreachable", () -> null);
                    fail("shared lock should not have been acquired");
                } catch (IllegalStateException expected) {
                    // A zero timeout still performs one non-blocking attempt.
                }
            }
        }
    }

    // Positive timeouts retry while contended and succeed when the lock is released in time.
    @Test
    public void positiveTimeoutAcquiresAContendedLockAfterRelease() throws Exception {
        File file = temporaryFolder.newFile("waiting-metadata.cq4t");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.setLength(1);
            try (FileLock lock = randomAccessFile.getChannel()
                    .lock(Long.MAX_VALUE - 1, 1, false)) {
                Future<String> result = executor.submit(() -> {
                    started.countDown();
                    return SingleTableStore.doWithSharedLock(file, 2_000,
                            ignored -> "locked", () -> null);
                });
                assertTrue(started.await(1, TimeUnit.SECONDS));
                Thread.sleep(100);
                assertFalse("the shared-lock attempt must keep retrying while the lock is held", result.isDone());
                lock.release();
                assertEquals("locked", result.get(1, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
