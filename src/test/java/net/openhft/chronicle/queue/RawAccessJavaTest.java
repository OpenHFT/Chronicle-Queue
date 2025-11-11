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
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// For use with C++ RawAccessJava. Called from C++
public class RawAccessJavaTest extends QueueTestCommon {

    private final long QUEUE_HEADER_SIZE = 4;
    private final long RAW_SIZE_PREFIX = 4;

    private final long COUNT = 10;

    private boolean assert_from_cpp() {
        String env = System.getProperty("chronicle.test.env");
        return env != null && env.equals("from-cpp");
    }

    @Test
    public void Tailer() {
        if (!assert_from_cpp())
            return;

        String tmp = "/dev/shm/RawAccessCtoJ";
        System.out.println(tmp); // so C++ knows this ran rather than skipped

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build()) {

            ExcerptTailer tailer = cq.createTailer();

            for (int i = 0; i < COUNT; ++i) {
                try (DocumentContext dc = tailer.readingDocument()) {

                    Bytes<?> bytes = dc.wire().bytes();

                    bytes.readSkip(-QUEUE_HEADER_SIZE);
                    int header = bytes.readInt();

                    // document length, inc 4-byte length
                    int length = Wires.lengthOf(header);

                    // actual length of data
                    int data_length = bytes.readInt();

                    assertEquals(bytes.readByte(), (byte) 0xab);
                    assertEquals(bytes.readShort(), (short) 12);
                    assertEquals(bytes.readInt(), 123);
                    assertEquals(bytes.readLong(), 123456789L);
                    assertEquals(bytes.readFloat(), 1.234f, 1.0e-7);
                    assertEquals(bytes.readDouble(), 123.456, 1.0e-7);
                    assertEquals(bytes.readChar(), 'a');

                    StringBuilder sb = new StringBuilder();
                    bytes.read8bit(sb);
                    assertEquals(sb.toString(), "Hello World");
                }
            }
        }
    }

    @Test
    public void Appender() {
        if (!assert_from_cpp())
            return;

        String tmp = "/dev/shm/RawAccessJtoC";
        System.out.println(tmp); // so C++ knows this ran rather than skipped

        try (ChronicleQueue cq = SingleChronicleQueueBuilder.binary(tmp).build();

             ExcerptAppender appender = cq.createAppender()) {

            for (int i = 0; i < COUNT; ++i) {
                try (DocumentContext dc = appender.writingDocument()) {

                    Bytes<?> bytes = dc.wire().bytes();

                    // will contain the size of the blob
                    long start = bytes.writePosition();
                    bytes.writeSkip(RAW_SIZE_PREFIX);

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
                    bytes.writeInt(start, (int) (end - start - RAW_SIZE_PREFIX));
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
                assertTrue(dc.isPresent());
                Bytes<?> bytes = dc.wire().bytes();
                bytes.readSkip(-QUEUE_HEADER_SIZE);
                int header = bytes.readInt();
                int totalLength = Wires.lengthOf(header);
                int payloadLength = bytes.readInt();
                assertEquals("Length prefix should match payload content",
                        totalLength - RAW_SIZE_PREFIX, payloadLength);
            }
        } finally {
            IOTools.deleteDirWithFiles(dir, 2);
        }
    }

    private void writeInteropPayload(ExcerptAppender appender) {
        try (DocumentContext dc = appender.writingDocument()) {
            Bytes<?> bytes = dc.wire().bytes();
            long start = bytes.writePosition();
            bytes.writeSkip(RAW_SIZE_PREFIX);
            bytes.writeByte((byte) 0xab);
            bytes.writeShort((short) 12);
            bytes.writeInt(123);
            bytes.writeLong(123456789L);
            bytes.writeFloat(1.234f);
            bytes.writeDouble(123.456);
            bytes.writeChar('a');
            bytes.write8bit("Hello World");
            long end = bytes.writePosition();
            bytes.writeInt(start, (int) (end - start - RAW_SIZE_PREFIX));
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
                bytes.writeSkip(RAW_SIZE_PREFIX);
                long end = bytes.writePosition();
                bytes.writeInt(start, (int) (end - start - RAW_SIZE_PREFIX));
            }
            appender.writeText("follow-up");

            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isPresent());
                Bytes<?> bytes = dc.wire().bytes();
                bytes.readSkip(-QUEUE_HEADER_SIZE);
                bytes.readInt(); // header
                int payloadLength = bytes.readInt();
                assertEquals(0, payloadLength);
                assertEquals(0, bytes.readRemaining());
            }

            assertEquals("follow-up", tailer.readText());
        } finally {
            IOTools.deleteDirWithFiles(dir, 2);
        }
    }
}
