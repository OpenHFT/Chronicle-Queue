/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SingleTableStoreForEachKeyGuardTest extends QueueTestCommon {

    // A later scan must see table entries appended through another mapped store instance.
    @Test
    public void laterScanSeesEntriesAppendedByAnotherStore() throws Exception {
        File directory = getTmpDir();
        assertTrue(directory.mkdirs());
        File tableFile = Files.createTempFile(directory.toPath(), "table", SingleTableStore.SUFFIX).toFile();

        try (TableStore<Metadata.NoMeta> writer =
                     SingleTableBuilder.binary(tableFile, Metadata.NoMeta.INSTANCE).build()) {
            try (LongValue initial = writer.acquireValueFor("initial-key", 1)) {
                assertEquals(1, initial.getValue());
            }

            MappedBytes scannerBytes = MappedBytes.mappedBytes(
                    tableFile, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, false);
            scannerBytes.singleThreadedCheckDisabled(true);

            try (SingleTableStore<Metadata.NoMeta> scanner = new SingleTableStore<>(
                    WireType.BINARY_LIGHT, scannerBytes, Metadata.NoMeta.INSTANCE)) {
                try (LongValue boundInitial = scanner.acquireValueFor("initial-key", -1)) {
                    assertNotNull(boundInitial);
                    final String latestKey = "later-key";
                    final long latestOffset;
                    try (LongValue value = writer.acquireValueFor(latestKey, 2)) {
                        assertEquals(2, value.getValue());
                        latestOffset = ((Byteable) value).offset();
                    }
                    assertTrue(latestOffset > 0);

                    // Model a local limit captured while another store was still appending the entry.
                    final long staleWriteLimit = latestOffset - 1;
                    scannerBytes.writePosition(staleWriteLimit);
                    scannerBytes.writeLimit(staleWriteLimit);
                    final long originalReadPosition = scannerBytes.readPosition();
                    final long originalReadLimit = scannerBytes.readLimit();

                    Set<String> keys = new HashSet<>();
                    scanner.forEachKey(keys, (result, key, value) -> {
                        result.add(key.toString());
                        value.int64();
                    });
                    assertTrue(keys.contains(latestKey));
                    assertEquals(originalReadPosition, scannerBytes.readPosition());
                    assertEquals(originalReadLimit, scannerBytes.readLimit());
                    assertEquals(staleWriteLimit, scannerBytes.writePosition());
                    assertEquals(staleWriteLimit, scannerBytes.writeLimit());
                    assertEquals(1, boundInitial.getValue());

                    RuntimeException failure = new IllegalStateException("callback failure");
                    assertSame(failure, assertThrows(IllegalStateException.class,
                            () -> scanner.forEachKey(null, (ignored, key, value) -> {
                                throw failure;
                            })));
                    assertEquals(originalReadPosition, scannerBytes.readPosition());
                    assertEquals(originalReadLimit, scannerBytes.readLimit());
                    assertEquals(staleWriteLimit, scannerBytes.writePosition());
                    assertEquals(staleWriteLimit, scannerBytes.writeLimit());
                    assertEquals(1, boundInitial.getValue());

                    for (int i = 0; i < 2000; i++) {
                        try (LongValue value = writer.acquireValueFor("growth-key-" + i, i)) {
                            assertEquals(i, value.getValue());
                        }
                    }
                    keys.clear();
                    scanner.forEachKey(keys, (result, key, value) -> {
                        result.add(key.toString());
                        value.int64();
                    });
                    assertEquals(2002, keys.size());
                    assertEquals(originalReadPosition, scannerBytes.readPosition());
                    assertEquals(originalReadLimit, scannerBytes.readLimit());
                    assertEquals(staleWriteLimit, scannerBytes.writePosition());
                    assertEquals(staleWriteLimit, scannerBytes.writeLimit());
                    assertEquals(1, boundInitial.getValue());
                }
            }
        }
    }

}
