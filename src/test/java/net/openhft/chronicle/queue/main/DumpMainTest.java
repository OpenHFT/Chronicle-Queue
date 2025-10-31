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

package net.openhft.chronicle.queue.main;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DumpMainTest extends QueueTestCommon {

    @Test
    public void dumpDirectoryPrintsQueueContents() {
        final File dir = getTmpDir();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(dir).build()) {
            // write a couple of simple messages
            try (DocumentContext dc = q.createAppender().writingDocument()) {
                dc.wire().write("msg").text("hello");
            }
            try (DocumentContext dc = q.createAppender().writingDocument()) {
                dc.wire().write("msg").text("world");
            }
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintStream out = new PrintStream(baos);
        DumpMain.dump(dir, out, Long.MAX_VALUE);
        final String dump = baos.toString();

        assertFalse("Dump should not be empty", dump.trim().isEmpty());
        assertTrue("Should include header with file path", dump.contains("## "));
        assertTrue("Should include first message", dump.contains("hello"));
        assertTrue("Should include second message", dump.contains("world"));
    }
}
