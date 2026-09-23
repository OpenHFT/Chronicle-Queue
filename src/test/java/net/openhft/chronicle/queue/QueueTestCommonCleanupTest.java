/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class QueueTestCommonCleanupTest extends QueueTestCommon {
    @Test
    public void acceptsAnAbsentDirectory() {
        File dir = getTmpDir();
        deleteDirAfterCleanup(dir);
        assertFalse(dir.exists());
    }

    @Test
    public void deletesFilesWithinTheDepthLimit() throws IOException {
        File dir = getTmpDir();
        Files.createDirectories(dir.toPath().resolve("one/two"));
        Files.createFile(dir.toPath().resolve("one/two/data"));

        deleteDirAfterCleanup(dir);

        assertFalse("Test directory should be absent after cleanup", dir.exists());
    }

    @Test
    public void retriesADeletionThatReturnsFalse() throws IOException {
        File dir = getTmpDir();
        Files.createDirectories(dir.toPath());
        Files.createFile(dir.toPath().resolve("data"));

        deleteDirAfterCleanup(failDirectoryDeletion(dir, 2));

        assertFalse("Cleanup should recover from transient deletion failures", dir.exists());
    }

    @Test(timeout = 5_000)
    public void reportsADirectoryThatCannotBeDeleted() throws IOException {
        File dir = getTmpDir();
        Files.createDirectories(dir.toPath());

        AssertionError error = assertThrows(AssertionError.class,
                () -> deleteDirAfterCleanup(failDirectoryDeletion(dir, Integer.MAX_VALUE)));

        assertTrue("Failure should identify the remaining directory", error.getMessage().contains(dir.getAbsolutePath()));
        assertTrue("Failure should describe the remaining entries", error.getMessage().contains("remaining entries"));
        assertTrue(dir.exists());
    }

    @Test
    public void preservesTheDepthLimit() throws IOException {
        File dir = getTmpDir();
        Files.createDirectories(dir.toPath().resolve("one/two/three"));

        assertThrows(AssertionError.class, () -> deleteDirAfterCleanup(dir));

        assertTrue("Cleanup must not delete deeper directories", dir.toPath().resolve("one/two/three").toFile().exists());
    }

    private static File failDirectoryDeletion(File dir, int failures) {
        // Model File.delete() returning false while keeping real directory contents and existence checks.
        return new File(dir.getPath()) {
            private static final long serialVersionUID = 1L;
            private int failuresRemaining = failures;

            @Override
            public boolean delete() {
                return failuresRemaining-- <= 0 && super.delete();
            }
        };
    }
}
