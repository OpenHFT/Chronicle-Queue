/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

public class TestEmptyFile {
    private final Path tmpDir = DirectoryUtils.tempDir(TestEmptyFile.class.getSimpleName()).toPath();

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
    @DisplayName("Empty store file does not yield a document")
    @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
    public void shouldHandleEmptyFile() {
        Assumptions.assumeFalse(OS.isWindows(), "Windows does not support this test");
        try (final ChronicleQueue queue =
                     ChronicleQueue.singleBuilder(tmpDir)
                             .testBlockSize()
                             .timeoutMS(100)
                             .readOnly(true)
                             .build()) {
            ExcerptTailer tailer = queue.createTailer();
            assertFalse(tailer.readingDocument().isPresent(), "empty file: no document");
        }
    }
}
