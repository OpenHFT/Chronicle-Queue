/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * First-increment platform test for the {@link QueueDirectoryWatcher} building block requested in
 * https://github.com/OpenHFT/Chronicle-Queue/issues/924
 * <p>
 * Restricted to Linux (inotify) so the assertions are deterministic and fast. macOS falls back to a
 * polling watch service with multi-second latency and Windows semantics differ, so those platforms are
 * left to a later increment (see the ignored design-intent test below).
 */
public class QueueDirectoryWatcherTest extends QueueTestCommon {

    private static final long TIMEOUT_MS = 5_000;

    @Test
    public void detectsCreationAndDeletionOfQueueFiles() throws IOException {
        assumeTrue("WatchService latency/semantics only deterministic on Linux inotify here", OS.isLinux());

        final File dir = getTmpDir();
        assertTrue(dir.isDirectory() || dir.mkdirs());
        final Path dirPath = dir.toPath();

        try (final QueueDirectoryWatcher watcher = new QueueDirectoryWatcher(dirPath)) {
            // nothing has happened yet
            assertFalse(watcher.hasPendingChange());

            // creating a queue file is detected
            final Path queueFile = dirPath.resolve("20240101.cq4");
            Files.createFile(queueFile);
            assertTrue("creation of a .cq4 file should be detected", waitForChange(watcher));

            // a non-queue file must NOT be reported as a change
            Files.createFile(dirPath.resolve("readme.txt"));
            assertTrue("creation of a non-.cq4 file must be filtered out", staysUnchanged(watcher));

            // deleting a queue file is detected
            Files.delete(queueFile);
            assertTrue("deletion of a .cq4 file should be detected", waitForChange(watcher));
        }
    }

    /**
     * Polls until a change is observed or the timeout elapses.
     */
    private static boolean waitForChange(final QueueDirectoryWatcher watcher) {
        final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (watcher.hasPendingChange())
                return true;
            pause();
        }
        return false;
    }

    /**
     * Returns true if no change is observed for a bounded window - long enough for an inotify event to
     * have arrived and been filtered out.
     */
    private static boolean staysUnchanged(final QueueDirectoryWatcher watcher) {
        final long deadline = System.currentTimeMillis() + 1_000;
        while (System.currentTimeMillis() < deadline) {
            if (watcher.hasPendingChange())
                return false;
            pause();
        }
        return true;
    }

    private static void pause() {
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Design-intent for the next increment: {@link TableDirectoryListing#refresh(boolean)} should
     * consult a {@link QueueDirectoryWatcher} and skip the full directory scan on a non-forced refresh
     * when no filesystem change has occurred, keeping the interval-driven scan only as a fallback. This
     * is ignored until that integration (and its macOS/Windows fallbacks) lands.
     */
    @Ignore("design intent for https://github.com/OpenHFT/Chronicle-Queue/issues/924 - integration not yet implemented")
    @Test
    public void refreshShouldConsultWatchServiceBeforeScanning() {
        // Intentionally empty: documents the intended end state (event-driven refresh with interval fallback).
    }
}
