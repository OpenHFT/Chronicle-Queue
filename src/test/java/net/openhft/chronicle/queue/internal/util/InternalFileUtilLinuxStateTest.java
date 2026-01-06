/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.util;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.util.FileState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InternalFileUtilLinuxStateTest extends QueueTestCommon {

    @Test
    @DisplayName("Linux file state reports open and closed correctly")
    public void stateOpenAndClosedOnLinux() throws Exception {
        Assumptions.assumeTrue(OS.isLinux(), "Test requires Linux to check /proc open files");
        Assumptions.assumeTrue(InternalFileUtil.getAllOpenFilesIsSupportedOnOS(),
                "Test requires /proc/self/fd support");

        final File dir = getTmpDir();
        // Ensure parent directory exists
        dir.mkdirs();
        final File f = new File(dir, "state-test.cq4");
        // Ensure file exists on disk
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            // Touch the file so it exists and the descriptor is active
            raf.write(0);
            // With the file open, it should be reported as OPEN
            final Map<String, String> openFiles = InternalFileUtil.getAllOpenFiles();
            assertEquals(FileState.OPEN, InternalFileUtil.state(f, openFiles),
                    "Open file should be reported as OPEN");
        }

        // After closing, re-snapshot and expect CLOSED
        final Map<String, String> openFiles2 = InternalFileUtil.getAllOpenFiles();
        assertEquals(FileState.CLOSED, InternalFileUtil.state(f, openFiles2),
                "Closed file should be reported as CLOSED");
    }
}
