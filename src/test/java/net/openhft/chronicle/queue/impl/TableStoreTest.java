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

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static org.junit.Assert.*;

public class TableStoreTest extends QueueTestCommon {
    @Test
    public void acquireValueFor() throws IOException {

        final File file = tempDir("table");
        Assume.assumeFalse("Ignored on hugetlbfs as byte offsets will be different due to page size", PageUtil.isHugePage(file.getAbsolutePath()));
        file.mkdir();

        final File tempFile = Files.createTempFile(file.toPath(), "table", SingleTableStore.SUFFIX).toFile();

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
                    "# 130972 bytes remaining\n", table.dump(WireType.BINARY_LIGHT));
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
                    "# 130956 bytes remaining\n", table.dump(WireType.BINARY_LIGHT));
        }
    }

    @Test
    public void acquireValueForReadOnly() throws IOException {

        final File file = tempDir("table");
        file.mkdir();

        final File tempFile = Files.createTempFile(file.toPath(), "table", SingleTableStore.SUFFIX).toFile();

        try (TableStore table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).build();
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(Long.MIN_VALUE, b.getVolatileValue());
            assertTrue(b.compareAndSwapValue(Long.MIN_VALUE, 2));
        }

        try (TableStore table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).readOnly(true).build();
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(2, b.getVolatileValue());
            assertThrows(IllegalStateException.class, () -> table.acquireValueFor("d"));
        }
    }
}