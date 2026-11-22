/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalTime;
import java.time.ZoneId;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SCQMetaRollMetadataTest extends QueueTestCommon {
    @Test
    public void readsPersistedEpochAndTimeZoneWithoutChangingTheTable() throws Exception {
        assertRoundTrip(LocalTime.of(6, 30), ZoneId.of("Europe/London"));
    }

    @Test
    public void preservesAbsentRollTimeAndZone() throws Exception {
        assertRoundTrip(null, null);
    }

    private void assertRoundTrip(LocalTime rollTime, ZoneId rollTimeZone) throws Exception {
        File directory = getTmpDir();
        Files.createDirectories(directory.toPath());
        File file = new File(directory, "metadata.cq4t");
        SCQMeta metadata = new SCQMeta(new SCQRoll(TestRollCycles.TEST_DAILY,
                12_345L, rollTime, rollTimeZone), 7);
        try (TableStore<SCQMeta> ignored = SingleTableBuilder.binary(file, metadata).build()) {
            assertEquals(7, ignored.metadata().sourceId());
        }
        BackgroundResourceReleaser.releasePendingResources();
        byte[] before = Files.readAllBytes(file.toPath());
        try (TableStore<?> store = SingleTableBuilder.binary(file, Metadata.NoMeta.INSTANCE)
                .readOnly(true).build()) {
            SCQMeta persisted = (SCQMeta) store.metadata();
            assertEquals(TestRollCycles.TEST_DAILY.lengthInMillis(), persisted.rollLengthInMillis());
            assertEquals(TestRollCycles.TEST_DAILY.format(), persisted.rollFormat());
            assertEquals(12_345L, persisted.rollEpoch());
            assertEquals(rollTime, persisted.rollTime());
            assertEquals(rollTimeZone, persisted.rollTimeZone());
        }
        BackgroundResourceReleaser.releasePendingResources();
        assertArrayEquals(before, Files.readAllBytes(file.toPath()));
    }
}
