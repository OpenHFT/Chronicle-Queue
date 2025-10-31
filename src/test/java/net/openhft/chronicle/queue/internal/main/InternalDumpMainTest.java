/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class InternalDumpMainTest extends QueueTestCommon {

    @Test
    public void shouldDumpDirectoryAndIncludeMetadataAndQueueFiles() throws Exception {
        final File dir = getTmpDir();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("hello");
            appender.writeText("world");
        }

        // locate a queue file to also test single-file dump
        final Path cq4 = Files.list(dir.toPath())
                .filter(p -> p.toString().endsWith(SingleChronicleQueue.SUFFIX))
                .findFirst().orElseThrow(() -> new AssertionError("no cq4 file in " + dir));

        // dump directory
        final ByteArrayOutputStream captureDir = new ByteArrayOutputStream();
        InternalDumpMain.dump(dir, new PrintStream(captureDir), Long.MAX_VALUE);
        final String outDir = captureDir.toString();
        assertTrue(outDir.contains("## "));
        assertTrue(outDir.contains(".cq4"));
        assertTrue(outDir.contains("metadata.cq4t"));

        // dump single file
        final ByteArrayOutputStream captureFile = new ByteArrayOutputStream();
        InternalDumpMain.dump(cq4.toFile(), new PrintStream(captureFile), Long.MAX_VALUE);
        assertNotEquals(0, captureFile.size());
    }
}

