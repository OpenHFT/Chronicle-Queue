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
import static org.junit.Assert.*;

public class PassiveTableLookupTest extends QueueTestCommon {
    @Test
    public void missingLookupAndCaseInsensitiveBindingDoNotChangeMetadata() throws Exception {
        File dir = getTmpDir();
        Files.createDirectories(dir.toPath());
        File file = new File(dir, "table.cq4t");
        try (TableStore<Metadata.NoMeta> table = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
            try (LongValue value = table.acquireValueFor("Existing", 42)) {
                assertEquals(42, value.getValue());
            }
            byte[] before = Files.readAllBytes(file.toPath());
            assertNull(table.getValueFor("missing"));
            try (LongValue value = table.getValueFor("EXISTING")) {
                assertEquals(42, value.getValue());
            }
            assertArrayEquals(before, Files.readAllBytes(file.toPath()));
        }
        try (TableStore<Metadata.NoMeta> table = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).readOnly(true).build()) {
            byte[] before = Files.readAllBytes(file.toPath());
            assertNull(table.getValueFor("missing"));
            try (LongValue value = table.getValueFor("existing")) {
                assertEquals(42, value.getValue());
            }
            assertArrayEquals(before, Files.readAllBytes(file.toPath()));
        }
    }
}
