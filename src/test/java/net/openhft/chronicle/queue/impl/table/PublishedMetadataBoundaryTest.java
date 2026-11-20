/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class PublishedMetadataBoundaryTest extends QueueTestCommon {
    @Test
    public void staleLimitAtPublishedHeaderDoesNotRewriteItsBinding() throws Exception {
        File file = new File(getTmpDir(), "table.cq4t");
        Files.createDirectories(file.toPath().getParent());
        try (TableStore<Metadata.NoMeta> writer = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE).build()) {
            MappedBytes bytes = MappedBytes.mappedBytes(file, OS.SAFE_PAGE_SIZE, OS.SAFE_PAGE_SIZE, false);
            try (SingleTableStore<Metadata.NoMeta> reader = new SingleTableStore<>(WireType.BINARY_LIGHT, bytes, Metadata.NoMeta.INSTANCE);
                 LongValue published = writer.doWithExclusiveLock(ts -> ts.acquireValueFor("published-heartbeat", 42))) {
                Wire wire = WireType.BINARY_LIGHT.apply(bytes);
                bytes.readPosition(0);
                bytes.readLimit(bytes.realCapacity());
                assertTrue(wire.readDataHeader());
                long recordStart = bytes.readPosition();
                int header = bytes.readVolatileInt(recordStart);
                assertTrue("the writer has published a complete record", header > 0);
                // Model a reader whose local bound predates the other store's publication.
                bytes.readPosition(0);
                bytes.writeLimit(recordStart);
                bytes.readLimit(recordStart);
                try (LongValue found = reader.doWithExclusiveLock(ts -> ts.acquireValueFor("published-heartbeat", -1))) {
                    assertEquals(42, found.getValue());
                }
                assertEquals("lookup must preserve the published value", 42, published.getValue());
                bytes.writeLimit(bytes.capacity());
                bytes.readLimit(bytes.realCapacity());
                assertEquals("lookup must preserve the record header", header, bytes.readVolatileInt(recordStart));
            }
        }
    }
}
