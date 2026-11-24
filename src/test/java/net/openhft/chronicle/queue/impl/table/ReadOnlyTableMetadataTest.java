/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadOnlyTableMetadataTest extends QueueTestCommon {
    @Test
    public void tableReportsActualMappingMode() throws Exception {
        File directory = getTmpDir();
        Files.createDirectories(directory.toPath());
        File file = new File(directory, "metadata.cq4t");
        try (TableStore<?> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build();
             LongValue value = store.acquireValueFor("sample", 42)) {
            assertFalse(store.readOnly());
            org.junit.Assert.assertEquals(42L, value.getValue());
        }
        BackgroundResourceReleaser.releasePendingResources();
        byte[] before = Files.readAllBytes(file.toPath());
        try (TableStore<?> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).readOnly(true).build()) {
            assertTrue("read-only mappings must select the non-writing lock path", store.readOnly());
        }
        BackgroundResourceReleaser.releasePendingResources();
        assertArrayEquals(before, Files.readAllBytes(file.toPath()));
    }
}
