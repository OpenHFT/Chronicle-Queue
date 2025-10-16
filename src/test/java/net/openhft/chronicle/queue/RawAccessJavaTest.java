/*
 * Copyright 2016-2025 chronicle.software
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
import net.openhft.chronicle.wire.Wires;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

// For use with C++ RawAccessJava. Called from C++
public class RawAccessJavaTest extends QueueTestCommon {

    final long QUEUE_HEADER_SIZE = 4;
    final long RAW_SIZE_PREFIX = 4;

    final long COUNT = 10;

    boolean assert_from_cpp() {
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
}
