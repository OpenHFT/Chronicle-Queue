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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

class TestEmptyFile {
    private Path tmpDir = DirectoryUtils.tempDir(TestEmptyFile.class.getSimpleName()).toPath();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    void setup() throws IOException {
        tmpDir.toFile().mkdirs();
        File file = tmpDir.resolve("20170320.cq4").toFile();
        new FileOutputStream(file).close();
    }

    @AfterEach
    void cleanup() {
        IOTools.deleteDirWithFiles(tmpDir.toFile());
    }

    @Test
    @Timeout(30)
    void shouldHandleEmptyFile() {
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
