/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.*;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.StreamCorruptedException;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class ToEndInvalidIndexTest extends QueueTestCommon {

    private static final long LAST_INDEX = RollCycles.FAST_DAILY.toIndex(2, 2);
    private Path queuePath;
    private SetTimeProvider setTimeProvider;


    @Before
    public void setUp() throws StreamCorruptedException {
        queuePath = IOTools.createTempDirectory("partialIndex");
        setTimeProvider = new SetTimeProvider();
        createQueueWithZeroFirstSubIndexValue(setTimeProvider, queuePath);
    }

    @After
    public void tearDown() {
        IOTools.deleteDirWithFiles(queuePath.toFile());
    }

    @Test
    public void testBackwardsToEndReportsCorrectIndex() {
        try (SingleChronicleQueue queue = createQueue(setTimeProvider, queuePath);
             ExcerptTailer tailer = queue.createTailer()) {

            StoreTailer storeTailer = (StoreTailer) tailer;
            assertEquals(0, storeTailer.store.indexing.linearScanCount());

            // Moving toEnd results in linearScan because we have a zero value in the first sub-index.
            tailer.direction(TailerDirection.BACKWARD).toEnd();

            assertEquals(1, storeTailer.store.indexing.linearScanCount());
        }
    }

    /**
     * Create a queue where there is multiple sub-indexes and the last sub-index is zero. Requires
     * n * 64 * sub-index-count * 2 excerpts.
     */
    private static void createQueueWithZeroFirstSubIndexValue(SetTimeProvider setTimeProvider, Path path) throws StreamCorruptedException {
        try (SingleChronicleQueue queue = createQueue(setTimeProvider, path);
             ExcerptAppender appender = queue.createAppender();) {

            Bytes<Void> bytes = Bytes.allocateElasticDirect();

            bytes.write("x");
            int count = queue.indexCount() * queue.indexSpacing() + 1;
            for (int x = 0; x < count; x++) {
                appender.writeBytes(bytes);
            }

            long index = appender.lastIndexAppended();
            long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
            StoreAppender storeAppender = (StoreAppender) appender;
            SingleChronicleQueueStore store = storeAppender.store;
            assert store != null;

            // Mess up the first sub-index
            store.indexing.setPositionForSequenceNumber(storeAppender, sequenceNumber, 0);
        }
    }

    @NotNull
    private static SingleChronicleQueue createQueue(SetTimeProvider setTimeProvider, Path queuePath) {
        return SingleChronicleQueueBuilder
                .binary(queuePath)
                .timeProvider(setTimeProvider)
                .testBlockSize()
                .rollCycle(RollCycles.FAST_DAILY)
                .build();
    }
}
