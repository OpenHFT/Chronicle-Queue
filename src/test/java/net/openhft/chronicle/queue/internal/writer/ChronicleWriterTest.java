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

package net.openhft.chronicle.queue.internal.writer;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Mocker;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

public class ChronicleWriterTest extends QueueTestCommon {
    private static final String METHOD_NAME = "doit";
    private final String cw1;
    private final String cw2;
    private final String cw3;
    private final File dir;

    public ChronicleWriterTest() throws FileNotFoundException {
        cw1 = IOTools.urlFor(this.getClass().getClassLoader(), "chronicle-writer1.yaml").getPath();
        cw2 = IOTools.urlFor(this.getClass().getClassLoader(), "chronicle-writer2.yaml").getPath();
        cw3 = IOTools.urlFor(this.getClass().getClassLoader(), "chronicle-writer3.yaml").getPath();
        dir = IOTools.createTempFile(this.getClass().getSimpleName());
    }

    @Test(timeout = 5000)
    public void testWireMarshallingMapAndDTO() throws IOException {
        ChronicleWriter chronicleWriter = chronicleWriter(null, cw1, cw2);
        chronicleWriter.execute();

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir).build()) {
            StringBuilder sb = new StringBuilder();
            @NotNull MethodReader mr = queue.createTailer().methodReader(Mocker.intercepting(MyInterface.class, "*", sb::append));
            Assert.assertTrue(mr.readOne());
            Assert.assertTrue(mr.readOne());
            Assert.assertFalse(mr.readOne());
            Assert.assertEquals("*doit[!net.openhft.chronicle.queue.internal.writer.ChronicleWriterTest$DTO {\n" +
                    "  age: 19,\n" +
                    "  name: Henry\n" +
                    "}\n" +
                    "]*doit[!net.openhft.chronicle.queue.internal.writer.ChronicleWriterTest$DTO {\n" +
                    "  age: 42,\n" +
                    "  name: Percy\n" +
                    "}\n" +
                    "]",sb.toString());
        } finally {
            IOTools.deleteDirWithFiles(dir);
        }
    }

    @Test(timeout = 5000)
    public void testWireMarshallingWithInterface() throws IOException {
        ChronicleWriter chronicleWriter = chronicleWriter(MyInterface.class.getTypeName(), cw2);
        chronicleWriter.execute();

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir).build()) {
            StringBuilder sb = new StringBuilder();
            @NotNull MethodReader mr = queue.createTailer().methodReader(Mocker.intercepting(MyInterface.class, "*", sb::append));
            Assert.assertTrue(mr.readOne());
            Assert.assertFalse(mr.readOne());
            Assert.assertEquals("*doit[!net.openhft.chronicle.queue.internal.writer.ChronicleWriterTest$DTO {\n" +
                    "  age: 42,\n" +
                    "  name: Percy\n" +
                    "}\n" +
                    "]",sb.toString());
        } finally {
            IOTools.deleteDirWithFiles(dir);
        }
    }

    @Test(timeout = 5000)
    public void testBytesMarshallingWithInterface() throws IOException {
        ChronicleWriter chronicleWriter = chronicleWriter(MyInterface2.class.getTypeName(), cw3);
        chronicleWriter.execute();

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir).build()) {
            StringBuilder sb = new StringBuilder();
            @NotNull MethodReader mr = queue.createTailer().methodReader(Mocker.intercepting(MyInterface2.class, "*", sb::append));
            Assert.assertTrue(mr.readOne());
            Assert.assertFalse(mr.readOne());
            Assert.assertEquals("*doit[!net.openhft.chronicle.queue.internal.writer.ChronicleWriterTest$DTO2 {\n" +
                    "  age: 42,\n" +
                    "  name: Percy\n" +
                    "}\n" +
                    "]",sb.toString());
        } finally {
            IOTools.deleteDirWithFiles(dir);
        }
    }

    private ChronicleWriter chronicleWriter(String interfaceName, String... files) {
        ChronicleWriter chronicleWriter = new ChronicleWriter().
                withBasePath(dir.toPath()).
                withMethodName(METHOD_NAME).
                withFiles(Arrays.asList(files));
        if (interfaceName != null)
            chronicleWriter.asMethodWriter(interfaceName);
        return chronicleWriter;
    }

    public interface MyInterface {
        void doit(DTO dto);
    }

    public interface MyInterface2 {
        void doit(DTO2 dto);
    }

    public static class DTO extends SelfDescribingMarshallable {
        private int age;
        private String name;
    }

    public static class DTO2 extends DTO {
        @Override
        public boolean usesSelfDescribingMessage() {
            return false;
        }
    }
}
