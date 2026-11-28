/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import java.util.Arrays;
import java.util.Objects;
import static org.junit.Assert.assertThrows;
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


    @Test
    public void cycleForFileUsesPersistedNonZeroEpochAfterRestart() {
        for (RollCycle rollCycle : new RollCycle[]{TestRollCycles.TEST_DAILY, TestRollCycles.TEST_HOURLY, RollCycles.WEEKLY}) {
            final File directory = getTmpDir();
            final long epoch = rollCycle.defaultEpoch() + 3L * rollCycle.lengthInMillis();
            final SetTimeProvider time = new SetTimeProvider();
            time.currentTimeMillis(epoch);
            final File firstRoll;
            final File laterRoll;
            try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(directory)
                    .rollCycle(rollCycle).epoch(epoch).timeProvider(time).testBlockSize().build()) {
                final StoreAppender appender = (StoreAppender) queue.acquireAppender();
                appender.writeText("cycle-zero");
                firstRoll = appender.store.file();
                time.advanceMillis(2L * rollCycle.lengthInMillis());
                appender.writeText("cycle-two");
                laterRoll = appender.store.file();
            }

            // Deliberately omit the roll cycle and epoch so metadata must supply both on restart.
            expectException("Overriding roll epoch from existing metadata, was 0, overriding to " + epoch);
            try (SingleChronicleQueue reopened = SingleChronicleQueueBuilder.single(directory)
                    .timeProvider(time).testBlockSize().build()) {
                assertEquals(epoch, reopened.epoch());
                assertEquals(rollCycle.lengthInMillis(), reopened.rollCycle().lengthInMillis());
                assertEquals(rollCycle.format(), reopened.rollCycle().format());
                assertEquals(0, reopened.cycleForFile(firstRoll));
                assertEquals(2, reopened.cycleForFile(laterRoll));
            }
        }
    }

    // Cycle parsing must use the Queue's persisted format and reject non-roll paths.
    @Test
    public void cycleForFileUsesTheQueueRollGeometry() {
        SetTimeProvider time = new SetTimeProvider(1_700_000_000_000L);
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.single(getTmpDir())
                .rollCycle(TestRollCycles.TEST_DAILY)
                .timeProvider(time)
                .build()) {
            queue.createAppender().writeText("one");
            time.advanceMillis(TestRollCycles.TEST_DAILY.lengthInMillis() * 2L);
            queue.createAppender().writeText("two");
            File[] rolls = Objects.requireNonNull(queue.file().listFiles(
                    file -> file.getName().endsWith(SingleChronicleQueue.SUFFIX)));
            Arrays.sort(rolls);
            File roll = rolls[rolls.length - 1];

            assertEquals(queue.lastCycle(), queue.cycleForFile(roll));
            assertThrows(IllegalArgumentException.class,
                    () -> queue.cycleForFile(new File(queue.file(), "metadata.cq4t")));
        }
    }
}
