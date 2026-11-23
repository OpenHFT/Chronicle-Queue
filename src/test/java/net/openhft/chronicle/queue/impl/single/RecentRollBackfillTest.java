/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_DAILY;
import static net.openhft.chronicle.wire.Wires.*;
import static org.junit.Assert.*;

public class RecentRollBackfillTest extends QueueTestCommon {
    private final SetTimeProvider time = new SetTimeProvider(0);
    private String original;

    @Before
    public void optIn() {
        original = System.getProperty(RecentRollBackfill.PROPERTY);
        System.setProperty(RecentRollBackfill.PROPERTY, "true");
    }

    @After
    public void restore() {
        if (original == null)
            System.clearProperty(RecentRollBackfill.PROPERTY);
        else
            System.setProperty(RecentRollBackfill.PROPERTY, original);
    }

    private SingleChronicleQueue open(File dir) {
        return SingleChronicleQueueBuilder.binary(dir).rollCycle(TEST4_DAILY).timeProvider(time).build();
    }

    @Test
    public void anyMatchingSinkCanExtendButOrdinaryWritesCannot() {
        File dir = getTmpDir();
        long first;
        long later;
        try (SingleChronicleQueue q = open(dir); ExcerptAppender a = q.createAppender()) {
            a.writeBytes(Bytes.from("one"));
            first = a.lastIndexAppended();
            time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            a.writeText("later");
            later = a.lastIndexAppended();
            InternalAppender app = (InternalAppender) a;
            System.clearProperty(RecentRollBackfill.PROPERTY);
            assertThrows(IllegalStateException.class, app::recentRollBackfill);
            assertThrows(ExactWriteBoundaryException.class, () -> app.writeBytes(first + 1, Bytes.from("two")));
            System.setProperty(RecentRollBackfill.PROPERTY, "true");
            RecentRollBackfill divergent = app.recentRollBackfill();
            assertThrows(IllegalArgumentException.class, () -> divergent.write(first, Bytes.from("bad")));
            assertThrows(IllegalStateException.class, () -> divergent.write(first + 1, Bytes.from("two")));
            RecentRollBackfill gap = app.recentRollBackfill();
            assertThrows(IllegalArgumentException.class, () -> gap.write(first + 1, Bytes.from("two")));
            RecentRollBackfill peer1 = app.recentRollBackfill();
            RecentRollBackfill peer2 = app.recentRollBackfill();
            assertEquals(first, peer1.firstIndex());
            peer1.write(first, Bytes.from("one"));
            peer2.write(first, Bytes.from("one"));
            peer1.write(first + 1, Bytes.from("two"));
            peer2.write(first + 1, Bytes.from("two"));
            peer2.write(first + 2, Bytes.from("three"));
            assertEquals(later, q.lastIndex());
            assertThrows(ExactWriteBoundaryException.class, () -> app.writeBytes(first + 3, Bytes.from("ordinary")));
            a.writeText("still current");
            assertEquals(later + 1, a.lastIndexAppended());
        }
        try (SingleChronicleQueue q = open(dir); ExcerptTailer t = q.createTailer(); ExcerptAppender a = q.createAppender()) {
            assertRecord(t, first, "one");
            assertRecord(t, first + 1, "two");
            assertRecord(t, first + 2, "three");
            assertTrue(t.moveToIndex(later));
            assertEquals("later", t.readText());
            assertEquals("still current", t.readText());
            time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            a.writeText("new roll");
            RecentRollBackfill stale = ((InternalAppender) a).recentRollBackfill();
            assertEquals(later, stale.firstIndex());
            assertThrows(IllegalArgumentException.class, () -> stale.write(first, Bytes.from("one")));
        }
    }

    @Test
    public void recoversAcrossSparseIndexPageBoundary() {
        File dir = getTmpDir();
        int count = TEST4_DAILY.defaultIndexCount() * TEST4_DAILY.defaultIndexSpacing();
        try (SingleChronicleQueue q = open(dir); ExcerptAppender a = q.createAppender()) {
            for (int i = 0; i < count; i++)
                a.writeBytes(Bytes.from("r" + i));
            time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            a.writeText("later");
            RecentRollBackfill peer = ((InternalAppender) a).recentRollBackfill();
            for (int i = 0; i <= count + 1; i++)
                peer.write(i, Bytes.from("r" + i));
            assertThrows(ExactWriteBoundaryException.class, () -> ((InternalAppender) a).writeBytes(count + 2, Bytes.from("ordinary")));
        }
        try (SingleChronicleQueue q = open(dir); ExcerptTailer t = q.createTailer()) {
            for (int i = count + 1; i >= 0; i--)
                assertRecord(t, i, "r" + i);
        }
    }

    @Test
    public void replayAfterSealMovedAndBeforePublicationRequiresMatchingBytes() throws Exception {
        File dir = getTmpDir();
        try (SingleChronicleQueue q = open(dir); ExcerptAppender appender = q.createAppender()) {
            appender.writeBytes(Bytes.from("one"));
            time.advanceMillis(TimeUnit.DAYS.toMillis(1));
            appender.writeText("later");
            try (SingleChronicleQueueStore store = q.storeForCycle(0, q.epoch(), false, null); MappedBytes bytes = store.bytes()) {
                long pos = store.writePosition();
                do {
                    pos += SPB_HEADER_SIZE + lengthOf(bytes.readVolatileInt(pos));
                    pos += BytesUtil.padOffset(pos);
                } while (isReadyMetaData(bytes.readVolatileInt(pos)));
                assertEquals(END_OF_DATA, bytes.readVolatileInt(pos));
                bytes.writePosition(pos + SPB_HEADER_SIZE);
                bytes.write(Bytes.from("two"));
                RecentRollBackfill.advanceSeal(bytes, pos, bytes.writePosition(), false);
                assertTrue(store.writePosition() < pos);
            }
        }
        try (SingleChronicleQueue q = open(dir); ExcerptAppender appender = q.createAppender(); ExcerptTailer t = q.createTailer()) {
            InternalAppender app = (InternalAppender) appender;
            assertThrows(ExactWriteBoundaryException.class, () -> app.writeBytes(1, Bytes.from("two")));
            RecentRollBackfill different = app.recentRollBackfill();
            different.write(0, Bytes.from("one"));
            assertThrows(IllegalStateException.class, () -> different.write(1, Bytes.from("bad")));
            RecentRollBackfill peer = app.recentRollBackfill();
            peer.write(0, Bytes.from("one"));
            peer.write(1, Bytes.from("two"));
            peer.write(2, Bytes.from("three"));
            assertRecord(t, 2, "three");
            assertThrows(ExactWriteBoundaryException.class, () -> app.writeBytes(3, Bytes.from("four")));
        }
    }

    private static void assertRecord(ExcerptTailer t, long index, String value) {
        assertTrue("index " + index, t.moveToIndex(index));
        try (DocumentContext dc = t.readingDocument()) {
            assertTrue(dc.isPresent());
            assertEquals(index, dc.index());
            assertEquals(value, dc.wire().bytes().toString());
        }
    }
}
