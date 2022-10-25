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

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableStoreTest extends QueueTestCommon {
    @Test
    public void acquireValueFor() throws IOException {
        expectException("Overwriting absent queue metadata");

        final File file = tempDir("table");
        file.mkdir();

        final File tempFile = Files.createTempFile((Path) file.toPath(), "table", SingleTableStore.SUFFIX).toFile();

        try (TableStore table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).build();
             LongValue a = table.acquireValueFor("a");
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(Long.MIN_VALUE, a.getVolatileValue());
            assertTrue(a.compareAndSwapValue(Long.MIN_VALUE, 1));
            assertEquals(Long.MIN_VALUE, b.getVolatileValue());
            assertTrue(b.compareAndSwapValue(Long.MIN_VALUE, 2));
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT\n" +
                    "}\n" +
                    "# position: 60, header: 0\n" +
                    "--- !!data #binary\n" +
                    "a: 1\n" +
                    "# position: 80, header: 1\n" +
                    "--- !!data #binary\n" +
                    "b: 2\n" +
                    "...\n" +
                    "# 130972 bytes remaining\n", table.dump());
        }

        try (TableStore table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).build();
             LongValue c = table.acquireValueFor("c");
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(Long.MIN_VALUE, c.getVolatileValue());
            assertTrue(c.compareAndSwapValue(Long.MIN_VALUE, 3));
            assertEquals(2, b.getVolatileValue());
            assertTrue(b.compareAndSwapValue(2, 22));
            assertEquals("--- !!meta-data #binary\n" +
                    "header: !STStore {\n" +
                    "  wireType: !WireType BINARY_LIGHT\n" +
                    "}\n" +
                    "# position: 60, header: 0\n" +
                    "--- !!data #binary\n" +
                    "a: 1\n" +
                    "# position: 80, header: 1\n" +
                    "--- !!data #binary\n" +
                    "b: 22\n" +
                    "# position: 96, header: 2\n" +
                    "--- !!data #binary\n" +
                    "c: 3\n" +
                    "...\n" +
                    "# 130956 bytes remaining\n", table.dump());
           // System.out.println(table.dump());
        }
    }

    @Test
    public void preCreateQueueMeta() throws IOException {
        expectException("Overwriting absent queue metadata");

        File basePath = Files.createTempDirectory("queueMeta").toFile();
        basePath.deleteOnExit();

        try (TableStore<Metadata.NoMeta> queue = SingleTableBuilder.builder(
                new File(basePath, "metadata.cq4t"), WireType.BINARY, Metadata.NoMeta.INSTANCE).build()) {
            // No-op.
        }

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(basePath).build();
             ExcerptAppender appender = queue.acquireAppender()) {
            appender.writeText("foo");
            appender.writeText("bar");
            appender.writeText("baz");
        }

        File metaFile = new File(basePath, "metadata.cq4t");
        assert metaFile.delete();
        new FileOutputStream(metaFile).close();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(basePath).build();
             ExcerptTailer tailer = queue.createTailer()) {
            tailer.toStart();
            assert "foo".equals(tailer.readText());
            assert "bar".equals(tailer.readText());
            assert "baz".equals(tailer.readText());

            System.err.println(queue.metaStore().metadata());
        }
    }
}