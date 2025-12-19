/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;

@RequiredForClient
public class DtoBytesMarshallableTest extends QueueTestCommon {

    @Test
    public void testDtoBytesMarshallable() {

        File tmp = getTmpDir();

        DtoBytesMarshallable dto = new DtoBytesMarshallable();

        dto.age = 45;
        dto.name.append("rob");

        try (ChronicleQueue q = ChronicleQueue.singleBuilder(tmp).build();
             final ExcerptAppender appender = q.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("who").object(dto);
            }

            try (DocumentContext dc = q.createTailer().readingDocument()) {

                DtoBytesMarshallable who = (DtoBytesMarshallable) dc.wire().read("who").object();
                Assertions.assertEquals("!net.openhft.chronicle.queue.DtoBytesMarshallableTest$DtoBytesMarshallable {\n" +
                        "  name: rob,\n" +
                        "  age: 45\n" +
                        "}\n", who.toString(), "dto: toString after roundtrip");
            }
        }
    }

    @Test
    public void testDtoAbstractMarshallable() {

        File tmp = getTmpDir();

        DtoAbstractMarshallable dto = new DtoAbstractMarshallable();

        dto.age = 45;
        dto.name.append("rob");

        try (ChronicleQueue q = ChronicleQueue.singleBuilder(tmp).build();
             final ExcerptAppender appender = q.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("who").object(dto);
            }

            try (DocumentContext dc = q.createTailer().readingDocument()) {
                String yaml = dc.toString();
                // System.out.println(yaml);

                DtoAbstractMarshallable who = (DtoAbstractMarshallable) dc.wire().read("who").object();
                // System.out.println(who);

                Assertions.assertTrue(yaml.contains(who.toString()), "yaml: should contain dto");
            }
        }
    }

    static class DtoBytesMarshallable extends BytesInBinaryMarshallable {

        final StringBuilder name = new StringBuilder();
        int age;

        @Override
        public void readMarshallable(BytesIn<?> bytes) {
            age = bytes.readInt();
            name.setLength(0);
            bytes.readUtf8(name);
        }

        @Override
        public void writeMarshallable(BytesOut<?> bytes) {
            bytes.writeInt(age);
            bytes.writeUtf8(name);
        }
    }

    static class DtoAbstractMarshallable extends SelfDescribingMarshallable {
        final StringBuilder name = new StringBuilder();
        int age;

        @Override
        public void readMarshallable(BytesIn<?> bytes) {
            age = bytes.readInt();
            name.setLength(0);
            bytes.readUtf8(name);
        }

        @Override
        public void writeMarshallable(BytesOut<?> bytes) {
            bytes.writeInt(age);
            bytes.writeUtf8(name);
        }
    }
}
