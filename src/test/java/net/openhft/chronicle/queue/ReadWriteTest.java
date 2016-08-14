/*
 * Copyright 2016 higherfrequencytrading.com
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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Jerry Shea on 14/08/16.
 */
public class ReadWriteTest {

    private static final String STR1 = "hello", STR2 = "hey";
    private File chroniclePath;

    @Before
    public void setup() {
        chroniclePath = new File(OS.TARGET, "read_only");
        try (SingleChronicleQueue readWrite = SingleChronicleQueueBuilder.binary(chroniclePath).readOnly(false).build()) {
            final ExcerptAppender appender = readWrite.acquireAppender();
            appender.writeText(STR1);
            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().bytes().writeUtf8(STR2);
            }
        }
    }

    @After
    public void teardown() {
        IOTools.shallowDeleteDirWithFiles(chroniclePath);
    }

    @Test
    public void testReadFromReadOnlyChronicle() {
        try (SingleChronicleQueue out = SingleChronicleQueueBuilder.binary(chroniclePath).readOnly(true).build()) {
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

    @Test(expected = IllegalStateException.class)
    public void testWriteToReadOnlyChronicle() {
        try (SingleChronicleQueue out = SingleChronicleQueueBuilder.binary(chroniclePath).readOnly(true).build()) {
            out.acquireAppender();
        }
    }
}
