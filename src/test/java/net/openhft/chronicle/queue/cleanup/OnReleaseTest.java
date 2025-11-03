/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.cleanup;

import net.openhft.chronicle.bytes.PageUtil;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.testframework.FlakyTestRunner;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static net.openhft.chronicle.queue.rollcycles.LegacyRollCycles.MINUTELY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OnReleaseTest extends QueueTestCommon {
    @Test
    public void onRelease() throws Throwable {
        FlakyTestRunner.builder(this::onRelease0).build().run();
    }

    private void onRelease0() {
        String path = OS.getTarget() + "/onRelease-" + Time.uniqueId();
        SetTimeProvider stp = new SetTimeProvider();
        AtomicInteger writeRoll = new AtomicInteger();
        AtomicInteger readRoll = new AtomicInteger();
        try (ChronicleQueue writeQ = SingleChronicleQueueBuilder
                .binary(path)
                .rollCycle(MINUTELY)
                .timeProvider(stp)
                .testBlockSize()
                .storeFileListener((c, f) -> {
                    System.out.println("write released " + f);
                    writeRoll.incrementAndGet();
                })
                .build();
             ChronicleQueue readQ = SingleChronicleQueueBuilder
                     .binary(path)
                     .rollCycle(MINUTELY)
                     .timeProvider(stp)
                     .testBlockSize()
                     .storeFileListener((c, f) -> {
                         System.out.println("read released " + f);
                         readRoll.incrementAndGet();
                     })
                     .build();
             ExcerptAppender appender = writeQ.createAppender();
             ExcerptTailer tailer = readQ.createTailer()) {

            // On hugetlbfs creating a large number of cycle files without unmapping them will cause page exhaustion in CI
            int messageCount = PageUtil.isHugePage(path) ? 10 : 500;
            for (int i = 0; i < messageCount; i++) {
                appender.writeText("hello-" + i);
                assertNotNull(tailer.readText());
                BackgroundResourceReleaser.releasePendingResources();
                assertEquals(i, writeRoll.get());
                assertEquals(i, readRoll.get());
                stp.advanceMillis(66_000);
            }
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }
}
