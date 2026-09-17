/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.wire.Wires;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class StoreAcquisitionDeletionTest extends QueueTestCommon {
    @Test
    public void readerDoesNotRecreateCycleDeletedDuringMapping() throws IOException {
        assertDeletedCycleRecovery(1, TailerDirection.FORWARD, false, 0, 2);
    }

    @Test
    public void newTailerRecoversWhenFirstCycleIsDeletedDuringMapping() throws IOException {
        assertDeletedCycleRecovery(0, TailerDirection.FORWARD, false, 1, 2);
    }

    @Test
    public void backwardReaderDoesNotRecreateLastCycleDuringToEnd() throws IOException {
        assertDeletedCycleRecovery(2, TailerDirection.BACKWARD, true, 1, 0);
    }

    @Test
    public void forwardToEndDoesNotRecreateLastCycle() throws IOException {
        assertDeletedCycleRecovery(2, TailerDirection.FORWARD, true);
    }

    private void assertDeletedCycleRecovery(int deletedCycle, TailerDirection direction, boolean toEnd, int... expected) throws IOException {
        Path directory = getTmpDir().toPath();
        createFixture(directory);
        final Path deletedFile = directory.resolve(String.format("19700101-00%d0X.cq4", deletedCycle));
        final boolean[] deleted = {false};
        SingleChronicleQueueBuilder builder = builder(directory);
        builder.preBuild();
        try (SingleChronicleQueue queue = new SingleChronicleQueue(builder) {
            @Override
            MappedFile mappedFile(File file, boolean readOnlyMapping) throws FileNotFoundException {
                if (!deleted[0] && file.toPath().equals(deletedFile)) {
                    try {
                        Files.delete(deletedFile);
                        deleted[0] = true;
                    } catch (IOException e) {
                        throw Jvm.rethrow(e);
                    }
                }
                return super.mappedFile(file, readOnlyMapping);
            }
        }; ExcerptTailer tailer = queue.createTailer()) {
            if (deletedCycle == 0)
                assertEquals(queue.rollCycle().toIndex(1, 0), tailer.index());
            if (toEnd) {
                tailer.direction(direction).toEnd();
                if (direction == TailerDirection.FORWARD)
                    assertEquals(queue.rollCycle().toIndex(1, 1), tailer.index());
            }
            for (int cycle : expected) {
                assertEquals("record-" + cycle, tailer.readText());
                assertEquals(queue.rollCycle().toIndex(cycle, 0), tailer.lastReadIndex());
            }
            assertNull(tailer.readText());
            assertTrue("The real cycle must be deleted at the mapping boundary", deleted[0]);
            assertFalse("A reader must not recreate the deleted file", Files.exists(deletedFile));
        }
    }

    @Test
    public void readerWaitsForZeroHeaderPublication() throws IOException {
        assertHeaderPublication(0);
    }

    @Test
    public void readerWaitsForIncompleteHeaderPublication() throws IOException {
        assertHeaderPublication(Wires.NOT_COMPLETE);
    }

    private void assertHeaderPublication(int incompleteHeader) throws IOException {
        Path directory = getTmpDir().toPath();
        createFixture(directory);
        File cycle = directory.resolve("19700101-0010X.cq4").toFile();
        try (MappedBytes bytes = MappedBytes.mappedBytes(cycle, OS.pageSize())) {
            int publishedHeader = bytes.readVolatileInt(0);
            bytes.writeVolatileInt(0, incompleteHeader);
            try (SingleChronicleQueue queue = builder(directory).build();
                 ExcerptTailer tailer = queue.createTailer()) {
                assertEquals("record-0", tailer.readText());
                assertNull("A reader must await the earlier cycle's publication", tailer.readText());
                assertEquals("The reader must not recover or replace the writer's header",
                        incompleteHeader, bytes.readVolatileInt(0));
                bytes.writeVolatileInt(0, publishedHeader);
                assertEquals("record-1", tailer.readText());
                assertEquals("record-2", tailer.readText());
                assertNull(tailer.readText());
            } finally {
                bytes.writeVolatileInt(0, publishedHeader);
            }
        }
    }

    @Test
    public void appenderCanWriteAfterTailerAcquiresCycle() {
        Path directory = getTmpDir().toPath();
        try (SingleChronicleQueue queue = builder(directory).build();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("first");
        }
        try (SingleChronicleQueue queue = builder(directory).build();
             ExcerptTailer tailer = queue.createTailer()) {
            assertEquals("first", tailer.readText());
            try (ExcerptAppender appender = queue.createAppender()) {
                appender.writeText("second");
            }
            assertEquals("second", tailer.readText());
            assertNull(tailer.readText());
        }
    }

    private void createFixture(Path directory) {
        SetTimeProvider time = new SetTimeProvider();
        try (SingleChronicleQueue queue = builder(directory).timeProvider(time).build();
             ExcerptAppender appender = queue.createAppender()) {
            for (int cycle = 0; cycle < 3; cycle++) {
                appender.writeText("record-" + cycle);
                time.advanceMillis(TimeUnit.MINUTES.toMillis(10));
            }
        }
        BackgroundResourceReleaser.releasePendingResources();
    }

    private SingleChronicleQueueBuilder builder(Path directory) {
        return SingleChronicleQueueBuilder.binary(directory)
                .rollCycle(RollCycles.TEN_MINUTELY)
                .testBlockSize()
                .timeoutMS(0);
    }
}
