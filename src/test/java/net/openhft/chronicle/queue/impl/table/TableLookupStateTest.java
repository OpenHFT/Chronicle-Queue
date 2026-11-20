/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class TableLookupStateTest extends QueueTestCommon {
    @Test
    public void existingLookupRestoresCursorsAfterAnotherMappingGrows() throws Exception {
        File directory = getTmpDir();
        Files.createDirectories(directory.toPath());
        File file = new File(directory, "table.cq4t");
        try (TableStore<Metadata.NoMeta> writer = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
            MappedBytes bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, false);
            try (SingleTableStore<Metadata.NoMeta> reader = new SingleTableStore<>(WireType.BINARY_LIGHT, bytes, Metadata.NoMeta.INSTANCE)) {
                for (int i = 0; i < 2000; i++) {
                    try (LongValue value = writer.acquireValueFor("key-" + i, i)) {
                        assertEquals(i, value.getValue());
                    }
                }
                bytes.writePosition(256);
                bytes.writeLimit(256);
                bytes.readLimit(128);
                bytes.readPosition(16);
                long writePosition = bytes.writePosition();
                try (LongValue value = reader.acquireValueFor("key-1999", -1)) {
                    assertEquals(1999, value.getValue());
                }
                assertEquals(16, bytes.readPosition());
                assertEquals(128, bytes.readLimit());
                assertEquals(writePosition, bytes.writePosition());
                assertEquals(256, bytes.writeLimit());
            }
        }
    }
}
