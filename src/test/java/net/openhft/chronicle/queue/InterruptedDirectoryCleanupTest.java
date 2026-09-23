/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@SuppressWarnings({"try", "serial"})
public class InterruptedDirectoryCleanupTest extends QueueTestCommon {
    @Test
    public void bodyFailureSurvivesDeletionFailureAndOtherDirectoriesAreAttempted() throws Exception {
        File first = getTmpDir();
        File last = getTmpDir();
        Files.createDirectories(first.toPath());
        Files.createDirectories(last.toPath());
        AssertionError body = new AssertionError("body");
        AssertionError cleanup = new AssertionError("cleanup");
        File failing = new File(first.getPath()) {
            @Override
            public boolean delete() {
                throw cleanup;
            }
        };
        Throwable actual = assertThrows(AssertionError.class, () -> {
            try (FixtureCleanup ignored = new FixtureCleanup(
                    () -> deleteDirAfterCleanup(failing), () -> deleteDirAfterCleanup(last))) {
                throw body;
            }
        });
        assertSame(body, actual);
        assertArrayEquals(new Throwable[]{cleanup}, body.getSuppressed());
        assertFalse("A failed deletion must not abandon remaining owners", last.exists());
    }

    @Test
    public void initiallyInterruptedDeletionRetriesAndRestoresInterruption() throws Exception {
        checkRetry(false, false);
    }

    @Test
    public void repeatedInterruptsDoNotAbandonDeletion() throws Exception {
        checkRetry(true, false);
    }

    @Test
    public void repeatedInterruptsUseTheOriginalBudgetAndReportFailure() throws Exception {
        checkRetry(true, true);
    }

    private void checkRetry(boolean interruptEveryAttempt, boolean neverDeletes) throws Exception {
        File real = getTmpDir();
        Files.createDirectories(real.toPath());
        AtomicInteger attempts = new AtomicInteger();
        File transientFailure = new File(real.getPath()) {
            @Override
            public boolean delete() {
                if (interruptEveryAttempt)
                    Thread.currentThread().interrupt();
                return attempts.incrementAndGet() >= 3 && !neverDeletes && super.delete();
            }
        };
        long started = System.nanoTime();
        Thread.currentThread().interrupt();
        try {
            if (neverDeletes) {
                AssertionError failure = assertThrows(AssertionError.class, () -> deleteDirAfterCleanup(transientFailure));
                assertTrue(failure.getMessage().contains(real.getAbsolutePath()));
                assertTrue("Use the one-second budget despite interruptions",
                        System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(1000));
                assertTrue(real.exists());
                assertTrue(attempts.get() > 2);
            } else {
                deleteDirAfterCleanup(transientFailure);
                assertEquals(3, attempts.get());
                assertFalse(real.exists());
            }
            assertTrue("Restore the caller's interruption on success and failure", Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
