/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public final class FileModificationTimeTest extends QueueTestCommon {
    private final AtomicInteger fileCount = new AtomicInteger();

    private static void waitForDiff(final long a, final LongSupplier b) {
        final long timeout = System.currentTimeMillis() + 15_000L;
        while ((!Thread.currentThread().isInterrupted()) && System.currentTimeMillis() < timeout) {
            if (a != b.getAsLong()) {
                return;
            }
            Jvm.pause(1_000L);
        }

        fail("Values did not become different");
    }

    @Test
    @DisplayName("Directory modification time updates after file changes")
    public void shouldUpdateDirectoryModificationTime() {
        final File dir = getTmpDir();
        Assumptions.assumeFalse(PageUtil.isHugePage(dir.getAbsolutePath()),
                "Test requires a non-hugetlbfs directory");
        dir.mkdirs();

        final long startModTime = dir.lastModified();

        modifyDirectoryContentsUntilVisible(dir, startModTime);

        final long afterOneFile = dir.lastModified();
        assertNotEquals(startModTime, afterOneFile, "file mod time: updated after first file");

        modifyDirectoryContentsUntilVisible(dir, afterOneFile);
        final long afterTwoFiles = dir.lastModified();
        assertNotEquals(afterOneFile, afterTwoFiles, "file mod time: updated after second file");
    }

    private void modifyDirectoryContentsUntilVisible(final File dir, final long startTime) {
        waitForDiff(startTime, () -> {
            createFile(dir, fileCount.getAndIncrement() + ".txt");
            return dir.lastModified();
        });
    }

    private void createFile(
            final File dir, final String filename) {
        final File file = new File(dir, filename);
        try (final FileWriter writer = new FileWriter(file)) {

            writer.append("foo");
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create test file " + file, e);
        }
    }
}
