/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static net.openhft.chronicle.queue.DirectoryUtils.tempDir;
import static org.junit.jupiter.api.Assertions.*;

public class TableStoreTest extends QueueTestCommon {
    @Test
    @DisplayName("Acquire values write and persist table entries")
    public void acquireValueFor() throws IOException {

        final File file = tempDir("table");
        Assumptions.assumeFalse(PageUtil.isHugePage(file.getAbsolutePath()), "Ignored on hugetlbfs as byte offsets will be different due to page size");
        file.mkdir();

        final File tempFile = Files.createTempFile(file.toPath(), "table", SingleTableStore.SUFFIX).toFile();

        try (TableStore<Metadata.NoMeta> table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).build();
             LongValue a = table.acquireValueFor("a");
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(Long.MIN_VALUE, a.getVolatileValue(), "value for 'a' should start at Long.MIN_VALUE");
            assertTrue(a.compareAndSwapValue(Long.MIN_VALUE, 1),
                    "value for 'a' should CAS from Long.MIN_VALUE to 1");
            assertEquals(Long.MIN_VALUE, b.getVolatileValue(), "read-only setup should initialise 'b' at Long.MIN_VALUE");
            assertTrue(b.compareAndSwapValue(Long.MIN_VALUE, 2),
                    "read-only setup should CAS 'b' from Long.MIN_VALUE to 2");
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
                    "# 130972 bytes remaining\n", table.dump(WireType.BINARY_LIGHT),
                    "table dump should match expected initial values");
        }

        try (TableStore<Metadata.NoMeta> table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).build();
             LongValue c = table.acquireValueFor("c");
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(Long.MIN_VALUE, c.getVolatileValue(), "value for 'c' should start at Long.MIN_VALUE");
            assertTrue(c.compareAndSwapValue(Long.MIN_VALUE, 3),
                    "value for 'c' should CAS from Long.MIN_VALUE to 3");
            assertEquals(2, b.getVolatileValue(), "value for 'b' should persist from earlier write");
            assertTrue(b.compareAndSwapValue(2, 22), "value for 'b' should CAS from 2 to 22");
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
                    "# 130956 bytes remaining\n", table.dump(WireType.BINARY_LIGHT),
                    "table dump should match expected values after reopen");
        }
    }

    @Test
    @DisplayName("Read-only table retains values and blocks new keys")
    public void acquireValueForReadOnly() throws IOException {

        final File file = tempDir("table");
        file.mkdir();

        final File tempFile = Files.createTempFile(file.toPath(), "table", SingleTableStore.SUFFIX).toFile();

        try (TableStore<Metadata.NoMeta> table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).build();
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(Long.MIN_VALUE, b.getVolatileValue(), "value for 'b' should start at Long.MIN_VALUE");
            assertTrue(b.compareAndSwapValue(Long.MIN_VALUE, 2),
                    "value for 'b' should CAS from Long.MIN_VALUE to 2");
        }

        try (TableStore<Metadata.NoMeta> table = SingleTableBuilder.binary(tempFile, Metadata.NoMeta.INSTANCE).readOnly(true).build();
             LongValue b = table.acquireValueFor("b")) {
            assertEquals(2, b.getVolatileValue(), "value for 'b' should remain 2 in read-only mode");
            assertThrows(IllegalStateException.class, () -> table.acquireValueFor("d"),
                    "read-only table should reject acquiring new key 'd'");
        }
    }
}
