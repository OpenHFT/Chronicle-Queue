/*
 * Copyright 2014-2018 Chronicle Software
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.DirectoryUtils;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static org.junit.Assert.*;

public class TestEmptyFile {
    Path tmpDir = DirectoryUtils.tempDir(TestEmptyFile.class.getSimpleName()).toPath();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Before
    public void setup() throws Exception {
        tmpDir.toFile().mkdirs();
        Path templatePath = Paths.get(OS.USER_DIR, "src/test/resources/tr2/" + DirectoryListing.DIRECTORY_LISTING_FILE);
        Path to = tmpDir.resolve(templatePath.getFileName());
        Files.copy(templatePath, to, StandardCopyOption.REPLACE_EXISTING);
        File file = tmpDir.resolve("20170320.cq4").toFile();
        new FileOutputStream(file).close();
    }

    @After
    public void cleanup() {
        DirectoryUtils.deleteDir(tmpDir.toFile());
    }

    @Test
    @Ignore("This test  crashes JVM, but even with obvious fixes it will not pass as the queue doesn't know how to progress " +
            "past the empty/truncated file. Needs investigation if this is a supported situation - see issue #470")
    public void shouldHandleEmptyFile() {
        try (final SingleChronicleQueue queue =
                     SingleChronicleQueueBuilder.binary(tmpDir)
                             .testBlockSize()
                             .readOnly(true)
                             .build()) {
            ExcerptTailer tailer = queue.createTailer();
            assertFalse(tailer.readingDocument().isPresent());

            try (DocumentContext dc = SingleChronicleQueueBuilder.binary(tmpDir)
                    .testBlockSize().build().acquireAppender().writingDocument()) {
                dc.wire().write("hello").text("world");
            }

            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                assertEquals("world", dc.wire().read("hello").text());
            }
        }
    }
}
