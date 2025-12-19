/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wires;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// For use with C++ RawAccessJava. Called from C++
public class RawAccessJavaTest extends QueueTestCommon {

    private final long queueHeaderSize = 4;
    private final long rawSizePrefix = 4;

    private final long messageCount = 10;

    private boolean assert_from_cpp() {
        String env = System.getProperty("chronicle.test.env");
        return env != null && env.equals("from-cpp");
    }

    @Test
    public void tailerInterop() {
        if (!assert_from_cpp())
            return;

        String tmp = "/dev/shm/RawAccessCtoJ";
        System.out.println(tmp); // so C++ knows this ran rather than skipped

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build()) {

            ExcerptTailer tailer = cq.createTailer();

            for (int i = 0; i < messageCount; ++i) {
                try (DocumentContext dc = tailer.readingDocument()) {

                    Bytes<?> bytes = dc.wire().bytes();

                    bytes.readSkip(-queueHeaderSize);
                    int header = bytes.readInt();

                    // document length, inc 4-byte length
                    int length = Wires.lengthOf(header);

                    // actual length of data
                    int dataLength = bytes.readInt();

                    assertEquals((byte) 0xab, bytes.readByte(), "Byte value should match C++ written value 0xab");
                    assertEquals((short) 12, bytes.readShort(), "Short value should match C++ written value 12");
                    assertEquals(123, bytes.readInt(), "Int value should match C++ written value 123");
                    assertEquals(123456789L, bytes.readLong(), "Long value should match C++ written value 123456789");
                    assertEquals(1.234f, bytes.readFloat(), 1.0e-7, "Float value should match C++ written value 1.234");
                    assertEquals(123.456, bytes.readDouble(), 1.0e-7, "Double value should match C++ written value 123.456");
                    assertEquals('a', bytes.readChar(), "Char value should match C++ written value 'a'");

                    StringBuilder sb = new StringBuilder();
                    bytes.read8bit(sb);
                    assertEquals("Hello World", sb.toString(), "String value should match C++ written text 'Hello World'");
                }
            }
        }
    }

    @Test
    public void appenderInterop() {
        if (!assert_from_cpp())
            return;

        String tmp = "/dev/shm/RawAccessJtoC";
        System.out.println(tmp); // so C++ knows this ran rather than skipped

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();

             ExcerptAppender appender = cq.createAppender()) {

            for (int i = 0; i < messageCount; ++i) {
                try (DocumentContext dc = appender.writingDocument()) {

                    Bytes<?> bytes = dc.wire().bytes();

                    // will contain the size of the blob
                    long start = bytes.writePosition();
                    bytes.writeSkip(rawSizePrefix);

                    {
                        bytes.writeByte((byte) 0xab);
                        bytes.writeShort((short) 12);
                        bytes.writeInt(123);
                        bytes.writeLong(123456789L);
                        bytes.writeFloat(1.234f);
                        bytes.writeDouble(123.456);
                        bytes.writeChar('a');
                        bytes.write8bit("Hello World");
                    }

                    long end = bytes.writePosition();
                    bytes.writeInt(start, (int) (end - start - rawSizePrefix));
                }
            }
        }
    }

    @Test
    public void testLengthPrefixValidationWithoutCppInterop() {
        File dir = getTmpDir();
        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(dir.getAbsolutePath()).build();
             ExcerptAppender appender = cq.createAppender();
             ExcerptTailer tailer = cq.createTailer()) {

            writeInteropPayload(appender);

            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent(), "DocumentContext should be present after writing interop payload");
                Bytes<?> bytes = dc.wire().bytes();
                bytes.readSkip(-queueHeaderSize);
                int header = bytes.readInt();
                int totalLength = Wires.lengthOf(header);
                int payloadLength = bytes.readInt();
                assertEquals(totalLength - rawSizePrefix, payloadLength, "Length prefix should match payload content");
            }
        } finally {
            IOTools.deleteDirWithFiles(dir, 2);
        }
    }

    private void writeInteropPayload(ExcerptAppender appender) {
        try (DocumentContext dc = appender.writingDocument()) {
            Bytes<?> bytes = dc.wire().bytes();
            long start = bytes.writePosition();
            bytes.writeSkip(rawSizePrefix);
            bytes.writeByte((byte) 0xab);
            bytes.writeShort((short) 12);
            bytes.writeInt(123);
            bytes.writeLong(123456789L);
            bytes.writeFloat(1.234f);
            bytes.writeDouble(123.456);
            bytes.writeChar('a');
            bytes.write8bit("Hello World");
            long end = bytes.writePosition();
            bytes.writeInt(start, (int) (end - start - rawSizePrefix));
        }
    }

    @Test
    public void testZeroLengthInteropPayloadIsReadable() {
        File dir = getTmpDir();
        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(dir.getAbsolutePath()).build();
             ExcerptAppender appender = cq.createAppender();
             ExcerptTailer tailer = cq.createTailer()) {

            try (DocumentContext dc = appender.writingDocument()) {
                Bytes<?> bytes = dc.wire().bytes();
                long start = bytes.writePosition();
                bytes.writeSkip(rawSizePrefix);
                long end = bytes.writePosition();
                bytes.writeInt(start, (int) (end - start - rawSizePrefix));
            }
            appender.writeText("follow-up");

            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent(), "DocumentContext should be present after writing zero-length interop payload");
                Bytes<?> bytes = dc.wire().bytes();
                bytes.readSkip(-queueHeaderSize);
                bytes.readInt(); // header
                int payloadLength = bytes.readInt();
                assertEquals(0, payloadLength, "Payload length should be zero for empty interop message");
                assertEquals(0, bytes.readRemaining(), "No bytes should remain after reading zero-length payload");
            }

            assertEquals("follow-up", tailer.readText(), "Follow-up text should be readable after zero-length interop payload");
        } finally {
            IOTools.deleteDirWithFiles(dir, 2);
        }
    }
}
