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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SingleTableStoreForEachKeyGuardTest extends QueueTestCommon {

    //! A later scan must see table entries appended through another mapped store instance.
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

                try (LongValue value = scanner.getValueFor(latestKey)) {
                    assertNotNull(value);
                    assertEquals(2, value.getValue());
                }
                assertEquals(staleWriteLimit, scannerBytes.writeLimit());

                scannerBytes.writePosition(staleWriteLimit);
                scannerBytes.writeLimit(staleWriteLimit);
                Set<String> keys = new HashSet<>();
                scanner.forEachKey(keys, (result, key, value) -> {
                    result.add(key.toString());
                    value.int64();
                });
                assertTrue(keys.contains(latestKey));
                assertEquals(staleWriteLimit, scannerBytes.writeLimit());
            }
        }
    }

    //! Concurrent scans must restore their positions so later bound-value reads remain valid.
    @Test
    public void scansWhileAnotherStoreAppendsKeys() throws Exception {
        File directory = getTmpDir();
        assertTrue(directory.mkdirs());
        File tableFile = Files.createTempFile(directory.toPath(), "table", SingleTableStore.SUFFIX).toFile();

        try (TableStore<Metadata.NoMeta> writer =
                     SingleTableBuilder.binary(tableFile, Metadata.NoMeta.INSTANCE).build();
             TableStore<Metadata.NoMeta> scanner =
                     SingleTableBuilder.binary(tableFile, Metadata.NoMeta.INSTANCE).build()) {
            int keyCount = 2_000;
            CountDownLatch start = new CountDownLatch(1);
            AtomicBoolean writing = new AtomicBoolean(true);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> writes = executor.submit(() -> {
                    await(start);
                    try {
                        for (int i = 0; i < keyCount; i++) {
                            try (LongValue value = writer.acquireValueFor("key-" + i, i)) {
                                assertEquals(i, value.getValue());
                            }
                        }
                    } finally {
                        writing.set(false);
                    }
                });
                Future<?> scans = executor.submit(() -> {
                    await(start);
                    int scanCount = 0;
                    do {
                        scanner.forEachKey(new HashSet<>(), (keys, key, value) -> {
                            keys.add(key.toString());
                            value.int64();
                        });
                        scanCount++;
                    } while (writing.get() || scanCount < 1_000);
                });

                start.countDown();
                writes.get(30, TimeUnit.SECONDS);
                scans.get(30, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }

            Set<String> keys = new HashSet<>();
            scanner.forEachKey(keys, (result, key, value) -> {
                result.add(key.toString());
                value.int64();
            });
            assertEquals(keyCount, keys.size());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
