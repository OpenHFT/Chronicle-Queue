/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/** Explicit C++ interoperability entry points; see docs/cpp-interop.adoc. */
public class RawAccessJavaTest extends QueueTestCommon {
    static final int COUNT = 10;
    static final int PAYLOAD_LENGTH = 40;

    @Test
    public void Tailer() throws IOException {
        assumeTrue("External C++ fixture requires -Pcpp-interop", interopEnabled());
        readFixture(new File(fixtureRoot(), "cpp-to-java"), COUNT);
    }

    @Test
    public void Appender() throws IOException {
        assumeTrue("External C++ consumer requires -Pcpp-interop", interopEnabled());
        writeFixture(new File(fixtureRoot(), "java-to-cpp"), COUNT);
    }

    private static boolean interopEnabled() {
        return "from-cpp".equals(System.getProperty("chronicle.test.env"));
    }

    private static File fixtureRoot() throws IOException {
        String path = System.getProperty("chronicle.cpp.fixture.dir");
        assertNotNull("Set -Dchronicle.cpp.fixture.dir to a fresh per-run fixture directory", path);
        File root = new File(path);
        assertTrue("Fixture directory must already exist: " + root, root.isDirectory());
        String revision = System.getProperty("chronicle.cpp.revision");
        assertTrue("Pin -Dchronicle.cpp.revision to the 40-character C++ Git commit",
                revision != null && revision.matches("[0-9a-fA-F]{40}"));
        File marker = new File(root, "cpp-revision.txt");
        assertTrue("C++ producer must record its Git commit in " + marker, marker.isFile());
        assertEquals("Fixture was produced by a different C++ revision", revision,
                new String(Files.readAllBytes(marker.toPath()), StandardCharsets.US_ASCII).trim());
        return root;
    }

    static void readFixture(File directory, int count) {
        assertTrue("Missing C++ fixture directory: " + directory + "; run the pinned producer first", directory.isDirectory());
        File[] cycles = directory.listFiles((dir, name) -> name.endsWith(".cq4"));
        assertTrue("Missing C++ queue data in " + directory + "; run the pinned producer first",
                cycles != null && cycles.length > 0);
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory).readOnly(true).build();
             ExcerptTailer tailer = queue.createTailer()) {
            for (int i = 0; i < count; i++) {
                try (DocumentContext document = tailer.readingDocument()) {
                    assertTrue("Incomplete C++ fixture: missing record " + (i + 1) + " of " + count
                            + " in " + directory, document.isPresent());
                    Bytes<?> bytes = document.wire().bytes();
                    assertEquals("Unexpected raw payload length; do not add an inner length prefix",
                            PAYLOAD_LENGTH, bytes.readRemaining());
                    assertEquals((byte) 0xab, bytes.readByte());
                    assertEquals((short) 12, bytes.readShort());
                    assertEquals(123, bytes.readInt());
                    assertEquals(123456789L, bytes.readLong());
                    assertEquals(1.234f, bytes.readFloat(), 1.0e-7);
                    assertEquals(123.456, bytes.readDouble(), 1.0e-7);
                    assertEquals('a', bytes.readChar());
                    StringBuilder text = new StringBuilder();
                    bytes.read8bit(text);
                    assertEquals("Hello World", text.toString());
                    assertEquals("Trailing bytes in raw document", 0, bytes.readRemaining());
                }
            }
            try (DocumentContext extra = tailer.readingDocument()) {
                assertFalse("C++ fixture contains more than " + count + " records in " + directory,
                        extra.isPresent());
            }
        }
    }

    static void writeFixture(File directory, int count) {
        assertFalse("Output already exists; choose a fresh per-run fixture directory: " + directory,
                directory.exists());
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory).build();
             ExcerptAppender appender = queue.acquireAppender()) {
            for (int i = 0; i < count; i++) {
                try (DocumentContext document = appender.writingDocument()) {
                    writePayload(document.wire().bytes());
                }
            }
        }
    }

    static void writePayload(Bytes<?> bytes) {
        bytes.writeByte((byte) 0xab);
        bytes.writeShort((short) 12);
        bytes.writeInt(123);
        bytes.writeLong(123456789L);
        bytes.writeFloat(1.234f);
        bytes.writeDouble(123.456);
        bytes.writeChar('a');
        bytes.write8bit("Hello World");
    }
}
