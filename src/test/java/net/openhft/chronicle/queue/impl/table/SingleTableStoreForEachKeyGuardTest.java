/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
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
import static org.junit.Assert.assertTrue;

public class SingleTableStoreForEachKeyGuardTest extends QueueTestCommon {

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
