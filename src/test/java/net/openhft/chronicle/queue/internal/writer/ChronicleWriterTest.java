/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.writer;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Mocker;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

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

    @Test
    @DisplayName("Wire marshalling reads map and DTO messages from files")
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testWireMarshallingMapAndDTO() throws IOException {
        ChronicleWriter chronicleWriter = chronicleWriter(null, cw1, cw2);
        chronicleWriter.execute();

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir).build()) {
            StringBuilder sb = new StringBuilder();
            @NotNull MethodReader mr = queue.createTailer().methodReader(Mocker.intercepting(MyInterface.class, "*", sb::append));
            Assertions.assertTrue(mr.readOne(), "first message with Henry DTO should be read successfully");
            Assertions.assertTrue(mr.readOne(), "second message with Percy DTO should be read successfully");
            Assertions.assertFalse(mr.readOne(), "no more messages should remain after reading two DTOs");
            Assertions.assertEquals("*doit[!net.openhft.chronicle.queue.internal.writer.ChronicleWriterTest$DTO {\n" +
                    "  age: 19,\n" +
                    "  name: Henry\n" +
                    "}\n" +
                    "]*doit[!net.openhft.chronicle.queue.internal.writer.ChronicleWriterTest$DTO {\n" +
                    "  age: 42,\n" +
                    "  name: Percy\n" +
                    "}\n" +
                    "]", sb.toString(), "intercepted output should contain both Henry and Percy DTOs");
        } finally {
            IOTools.deleteDirWithFiles(dir);
        }
    }

    @Test
    @DisplayName("Wire marshalling reads interface DTO message")
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testWireMarshallingWithInterface() throws IOException {
        ChronicleWriter chronicleWriter = chronicleWriter(MyInterface.class.getTypeName(), cw2);
        chronicleWriter.execute();

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir).build()) {
            StringBuilder sb = new StringBuilder();
            @NotNull MethodReader mr = queue.createTailer().methodReader(Mocker.intercepting(MyInterface.class, "*", sb::append));
            Assertions.assertTrue(mr.readOne(), "single Percy DTO message should be read successfully via wire marshalling");
            Assertions.assertFalse(mr.readOne(), "no more messages should remain after reading single DTO");
            Assertions.assertEquals("*doit[!net.openhft.chronicle.queue.internal.writer.ChronicleWriterTest$DTO {\n" +
                    "  age: 42,\n" +
                    "  name: Percy\n" +
                    "}\n" +
                    "]", sb.toString(), "wire marshalled output should contain Percy DTO with age 42");
        } finally {
            IOTools.deleteDirWithFiles(dir);
        }
    }

    @Test
    @DisplayName("Bytes marshalling reads interface DTO message")
    @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
    public void testBytesMarshallingWithInterface() throws IOException {
        ChronicleWriter chronicleWriter = chronicleWriter(MyInterface2.class.getTypeName(), cw3);
        chronicleWriter.execute();

        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir).build()) {
            StringBuilder sb = new StringBuilder();
            @NotNull MethodReader mr = queue.createTailer().methodReader(Mocker.intercepting(MyInterface2.class, "*", sb::append));
            Assertions.assertTrue(mr.readOne(), "single Percy DTO2 message should be read successfully via bytes marshalling");
            Assertions.assertFalse(mr.readOne(), "no more messages should remain after reading single DTO2");
            Assertions.assertEquals("*doit[!net.openhft.chronicle.queue.internal.writer.ChronicleWriterTest$DTO2 {\n" +
                    "  age: 42,\n" +
                    "  name: Percy\n" +
                    "}\n" +
                    "]", sb.toString(), "bytes marshalled output should contain Percy DTO2 with age 42");
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
