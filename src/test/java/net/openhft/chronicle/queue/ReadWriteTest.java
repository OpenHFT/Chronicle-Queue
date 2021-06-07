/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

@RequiredForClient
public class ReadWriteTest extends QueueTestCommon {

    private static final String STR1 = "hello", STR2 = "hey";
    private File chroniclePath;

    @Before
    public void setup() {
        chroniclePath = new File(OS.getTarget(), "read_only_" + Time.uniqueId());
        try (ChronicleQueue readWrite = ChronicleQueue.singleBuilder(chroniclePath)
                .readOnly(false)
                .testBlockSize()
                .build()) {
            final ExcerptAppender appender = readWrite.acquireAppender();
            appender.writeText(STR1);
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().bytes().writeUtf8(STR2);
            }
        }
    }

    @After
    public void teardown() {
        try {
            IOTools.shallowDeleteDirWithFiles(chroniclePath);
        } catch (Exception e) {
            if (e instanceof AccessDeniedException && OS.isWindows())
                System.err.println(e);
            else
                throw e;
        }
    }

    @Test
    public void testReadFromReadOnlyChronicle() {
        assumeFalse(OS.isWindows());

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {
            // check dump
            assertTrue(out.dump().length() > 1);
            // and tailer
            ExcerptTailer tailer = out.createTailer();
            assertEquals(STR1, tailer.readText());
            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals(STR2, dc.wire().bytes().readUtf8());
                // even though this is read-only we can still call dc.wire().bytes().write... which causes java.lang.InternalError
                // Fixing this in a type-safe manner would require on Read/WriteDocumentContext to return WireIn/WireOut
            }
        }
    }

    @Test
    public void testNotInitializedMetadataFile() throws IOException {
        assumeFalse(OS.isWindows());

        final String expectedException = "Failback to readonly tablestore";
        expectException(expectedException);
        System.out.println("This test will produce a " + expectedException);

        File meta = new File(chroniclePath, "metadata.cq4t");
        assertTrue(meta.exists());

        try (RandomAccessFile raf = new RandomAccessFile(meta, "rw")) {
            raf.setLength(0);
        }

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {

            ExcerptTailer tailer = out.createTailer();
            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index);
        }
    }


    // Can't append to a read-only chronicle
    @Test(expected = IllegalStateException.class)
    public void testWriteToReadOnlyChronicle() {
        if (OS.isWindows()) {
            System.err.println("#460 Cannot test read only mode on windows");
            throw new IllegalStateException("not run");
        }

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {
            out.acquireAppender();
        }
    }

    @Test
    public void testToEndOnReadOnly() {
        assumeFalse("Read-only mode is not supported on Windows", OS.isWindows());

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(true)
                .build()) {
            ExcerptTailer tailer = out.createTailer();
            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index);
        }
    }

    @Test
    public void testNonWriteableFilesSetToReadOnly() {
        assumeFalse(OS.isWindows());
        final String expectedException = "Failback to readonly tablestore";
        expectException(expectedException);
        System.out.println("This test will produce a " + expectedException);

        Arrays.stream(chroniclePath.list()).forEach(s ->
                assertTrue(new File(chroniclePath, s).setWritable(false)));

        try (ChronicleQueue out = SingleChronicleQueueBuilder
                .binary(chroniclePath)
                .testBlockSize()
                .readOnly(false)
                .build()) {
            ExcerptTailer tailer = out.createTailer();
            tailer.toEnd();
            long index = tailer.index();
            assertNotEquals(0, index);
        }
    }
}
