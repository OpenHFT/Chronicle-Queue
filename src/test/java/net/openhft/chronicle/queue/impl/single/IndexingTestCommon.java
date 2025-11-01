/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

/**
 * Base test class for indexing related tests.
 */
public class IndexingTestCommon extends QueueTestCommon {

    protected SetTimeProvider timeProvider;
    protected SingleChronicleQueue queue;
    protected StoreAppender appender;
    protected List<Closeable> closeables;
    protected ExcerptTailer tailer;
    protected File queuePath;

    @BeforeEach
    public void before() {
        closeables = new ArrayList<>();
        timeProvider = new SetTimeProvider();
        queuePath = getTmpDir();
        IOTools.deleteDirWithFiles(queuePath);
        queue = createQueueInstance();
        appender = (StoreAppender) queue.createAppender();
        tailer = queue.createTailer();
    }

    @AfterEach
    public void after() {
        closeables.forEach(Closeable::closeQuietly);
        closeQuietly(appender, tailer, queue);
        IOTools.deleteDirWithFiles(queuePath);
    }

    protected SingleChronicleQueue createQueueInstance() {
        return SingleChronicleQueueBuilder.builder()
                .path(queuePath)
                .timeProvider(timeProvider)
                .rollCycle(rollCycle())
                .build();
    }

    protected RollCycle rollCycle() {
        return TestRollCycles.TEST_SECONDLY;
    }

    /**
     * @return indexing object for this queue that exposes common indexing operations.
     */
    public Indexing indexing(SingleChronicleQueue queue) {
        SingleChronicleQueueStore store = store(queue);
        return store.indexing;
    }

    /**
     * @return gets the store for the last cycle of the queue.
     */
    public SingleChronicleQueueStore store(SingleChronicleQueue queue) {
        SingleChronicleQueueStore store = queue.storeForCycle(queue.lastCycle(), queue.epoch(), false, null);
        closeables.add(store);
        return store;
    }
}
