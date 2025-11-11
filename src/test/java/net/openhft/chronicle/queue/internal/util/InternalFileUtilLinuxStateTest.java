/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.util;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.util.FileState;
import org.junit.Test;
import org.junit.Assume;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InternalFileUtilLinuxStateTest extends QueueTestCommon {

    @Test
    public void stateOpenAndClosedOnLinux() throws Exception {
        Assume.assumeTrue("Linux-only test", OS.isLinux());
        Assume.assumeTrue("/proc required", InternalFileUtil.getAllOpenFilesIsSupportedOnOS());

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
            assertEquals(FileState.OPEN, InternalFileUtil.state(f, openFiles));
        }

        // After closing, re-snapshot and expect CLOSED
        final Map<String, String> openFiles2 = InternalFileUtil.getAllOpenFiles();
        assertEquals(FileState.CLOSED, InternalFileUtil.state(f, openFiles2));
    }
}
