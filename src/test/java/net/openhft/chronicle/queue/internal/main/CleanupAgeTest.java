/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import org.junit.Test;
import static org.junit.Assert.*;
public class CleanupAgeTest {
    @Test public void ageBoundaryAndFutureTime() {
        long now = 1_000_000;
        assertTrue(InternalTruncateRollFilesMain.recentlyModified(now - 299_999, now, 300_000));
        assertFalse(InternalTruncateRollFilesMain.recentlyModified(now - 300_000, now, 300_000));
        assertTrue(InternalTruncateRollFilesMain.recentlyModified(now + 1, now, 300_000));
        assertFalse(InternalTruncateRollFilesMain.recentlyModified(now, now, 0));
        assertTrue(InternalTruncateRollFilesMain.recentlyModified(now + 1, now, 0));
    }
    @Test public void modificationAfterCaptureIsRefused() throws Exception {
        org.junit.Assume.assumeTrue("Linux file identity", net.openhft.chronicle.core.OS.isLinux());
        Path file = Files.createTempFile("cleanup-age-", ".cq4");
        try {
            Files.write(file, new byte[4096]);
            Files.setLastModifiedTime(file, FileTime.fromMillis(1_000_000));
            CycleFileIdentity identity = CycleFileIdentity.capture(file);
            Files.setLastModifiedTime(file, FileTime.fromMillis(1_000_001));
            assertThrows(java.io.IOException.class, () -> identity.check(file));
        } finally { Files.delete(file); }
    }
}
