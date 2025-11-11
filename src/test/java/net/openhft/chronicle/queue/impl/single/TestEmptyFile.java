/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.assertFalse;

public class TestEmptyFile {
    private Path tmpDir = DirectoryUtils.tempDir(TestEmptyFile.class.getSimpleName()).toPath();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Before
    public void setup() throws IOException {
        tmpDir.toFile().mkdirs();
        File file = tmpDir.resolve("20170320.cq4").toFile();
        new FileOutputStream(file).close();
    }

    @After
    public void cleanup() {
        IOTools.deleteDirWithFiles(tmpDir.toFile());
    }

    @Test(timeout = 30000)
    public void shouldHandleEmptyFile() {
        Assume.assumeFalse(OS.isWindows());
        try (final ChronicleQueue queue =
                     ChronicleQueue.singleBuilder(tmpDir)
                             .testBlockSize()
                             .timeoutMS(100)
                             .readOnly(true)
                             .build()) {
            ExcerptTailer tailer = queue.createTailer();
            assertFalse(tailer.readingDocument().isPresent());
        }
    }
}
