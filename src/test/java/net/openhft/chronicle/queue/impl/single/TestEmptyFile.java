/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

public class TestEmptyFile {
    private Path tmpDir = DirectoryUtils.tempDir(TestEmptyFile.class.getSimpleName()).toPath();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    public void setup() throws IOException {
        tmpDir.toFile().mkdirs();
        File file = tmpDir.resolve("20170320.cq4").toFile();
        new FileOutputStream(file).close();
    }

    @AfterEach
    public void cleanup() {
        IOTools.deleteDirWithFiles(tmpDir.toFile());
    }

    @Test
    @Timeout(value = 30_000, unit = TimeUnit.MILLISECONDS)
    public void shouldHandleEmptyFile() {
        assumeFalse(OS.isWindows());
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
