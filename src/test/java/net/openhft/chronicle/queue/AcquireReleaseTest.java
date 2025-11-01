/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RequiredForClient
public class AcquireReleaseTest extends QueueTestCommon {
    @Test
    public void testAcquireAndRelease() {
        File dir = IOTools.createTempDirectory("testAcquireAndRelease").toFile();

        AtomicInteger acount = new AtomicInteger();
        AtomicInteger qcount = new AtomicInteger();
        StoreFileListener sfl = new StoreFileListener() {
            @Override
            public void onAcquired(int cycle, File file) {
                // System.out.println("onAcquired(): " + file);
                acount.incrementAndGet();
            }

            @Override
            public void onReleased(int cycle, File file) {
                // System.out.println("onReleased(): " + file);
                qcount.incrementAndGet();
            }
        };
        AtomicLong time = new AtomicLong(1000L);
        TimeProvider tp = () -> time.getAndAccumulate(1000, (x, y) -> x + y);
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .storeFileListener(sfl)
                .timeProvider(tp)
                .build()) {

            int iter = 4;
            try (ExcerptAppender excerptAppender = queue.createAppender()) {
                for (int i = 0; i < iter; i++) {
                    excerptAppender.writeDocument(w -> {
                        w.write("a").marshallable(m -> {
                            m.write("b").text("c");
                        });
                    });
                }
            }

            BackgroundResourceReleaser.releasePendingResources();

            Assert.assertEquals(iter, acount.get());
            Assert.assertEquals(iter, qcount.get());
        }
        IOTools.deleteDirWithFiles(dir);
    }

    @Test
    public void testReserveAndRelease() {
        File dir = getTmpDir();

        SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(1000);
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize()
                .rollCycle(TEST_SECONDLY)
                .timeProvider(stp)
                .build();
             final ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("Hello World");
            stp.currentTimeMillis(2000);
            appender.writeText("Hello World");
            queue.createTailer().readText();
            try (ExcerptTailer tailer = queue.createTailer()) {
                tailer.readText();
                tailer.readText();
                tailer.readText();
            }
        }
        IOTools.deleteDirWithFiles(dir);
    }

    @Test
    public void testWithCleanupStoreFilesWithNoDataAcquireAndRelease() throws InterruptedException, ExecutionException {
        final File dir = getTmpDir();
        final SetTimeProvider stp = new SetTimeProvider();
        final AtomicInteger acount = new AtomicInteger();
        final AtomicInteger qcount = new AtomicInteger();
        final StoreFileListener storeFileListener = new StoreFileListener() {
            public void onAcquired(int cycle, File file) {
                acount.incrementAndGet();
            }

            @Override
            public void onReleased(int cycle, File file) {
                qcount.incrementAndGet();
            }
        };

        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .storeFileListener(storeFileListener)
                .timeProvider(stp)
                .rollCycle(TEST_SECONDLY)
                .build();
             // new appender created
             final ExcerptAppender appender = queue.createAppender()) {
            appender.writeText("Main thread: Hello world");
            BackgroundResourceReleaser.releasePendingResources();
            assertEquals(1, acount.get());

            stp.advanceMillis(1000L); // advance 1 cycle, so that cleanupStoreFilesWithNoData() acquires store

            // other appender is created
            try (final ExcerptAppender secondAppender = queue.createAppender()) {
                assertNotNull(secondAppender);
                // Here store is Acquired twice (second time in cleanupStoreFilesWithNoData())
                // Do nothing with it
            }
        }
        BackgroundResourceReleaser.releasePendingResources();

        assertEquals(2, acount.get());

        assertEquals(2, qcount.get());
        IOTools.deleteDirWithFiles(dir);
    }
}
