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

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

@RequiredForClient
public class DtoBytesMarshallableTest extends QueueTestCommon {

    @Test
    public void testDtoBytesMarshallable() {

        File tmp = getTmpDir();

        DtoBytesMarshallable dto = new DtoBytesMarshallable();

        dto.age = 45;
        dto.name.append("rob");

        try (ChronicleQueue q = ChronicleQueue.singleBuilder(tmp).build()) {

            try (DocumentContext dc = q.acquireAppender().writingDocument()) {
                dc.wire().write("who").object(dto);
            }

            try (DocumentContext dc = q.createTailer().readingDocument()) {

                DtoBytesMarshallable who = (DtoBytesMarshallable) dc.wire().read("who").object();
                Assert.assertEquals("!net.openhft.chronicle.queue.DtoBytesMarshallableTest$DtoBytesMarshallable {\n" +
                        "  name: rob,\n" +
                        "  age: 45\n" +
                        "}\n", who.toString());
            }
        }
 }

    @Test
    public void testDtoAbstractMarshallable() {

        File tmp = getTmpDir();

        DtoAbstractMarshallable dto = new DtoAbstractMarshallable();

        dto.age = 45;
        dto.name.append("rob");

        try (ChronicleQueue q = ChronicleQueue.singleBuilder(tmp).build()) {

            try (DocumentContext dc = q.acquireAppender().writingDocument()) {
                dc.wire().write("who").object(dto);
            }

            try (DocumentContext dc = q.createTailer().readingDocument()) {
                String yaml = dc.toString();
               // System.out.println(yaml);

                DtoAbstractMarshallable who = (DtoAbstractMarshallable) dc.wire().read("who").object();
               // System.out.println(who);

                Assert.assertTrue(yaml.contains(who.toString()));
            }
        }
    }

    public static class DtoBytesMarshallable extends BytesInBinaryMarshallable {

        StringBuilder name = new StringBuilder();
        int age;

        @SuppressWarnings("unchecked")
        public void readMarshallable(BytesIn<?> bytes) {
            age = bytes.readInt();
            name.setLength(0);
            bytes.readUtf8(name);
        }

        public void writeMarshallable(BytesOut<?> bytes) {
            bytes.writeInt(age);
            bytes.writeUtf8(name);
        }
    }

    public static class DtoAbstractMarshallable extends SelfDescribingMarshallable {
        StringBuilder name = new StringBuilder();
        int age;

        @SuppressWarnings("unchecked")
        public void readMarshallable(BytesIn<?> bytes) {
            age = bytes.readInt();
            name.setLength(0);
            bytes.readUtf8(name);
        }

        public void writeMarshallable(BytesOut<?> bytes) {
            bytes.writeInt(age);
            bytes.writeUtf8(name);
        }
    }
}
