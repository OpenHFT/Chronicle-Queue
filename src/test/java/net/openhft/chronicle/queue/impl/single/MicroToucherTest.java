/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class MicroToucherTest extends QueueTestCommon {

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void touchPageTestBlockSize() {
        touchPage(b -> b.blockSize(64 << 20), 66561);
    }

    private void touchPage(Consumer<SingleChronicleQueueBuilder> configure, int pagesExpected) {
        long start = System.nanoTime();
        String path = OS.getTarget() + "/touchPage-" + System.nanoTime();
        int pages = 0;
        final SingleChronicleQueueBuilder builder = ChronicleQueue.singleBuilder(path);
        configure.accept(builder);

        try (ChronicleQueue q = builder.build();
             final StoreAppender appender = (StoreAppender) q.createAppender()) {

            Thread msync = new Thread(() -> {
                try {
                    while (true) {
                        appender.bgMicroTouch();
                        Jvm.pause(25);
                    }
                } catch (ClosedIllegalStateException expected) {
                }
            });
            msync.setDaemon(true);
            msync.start();

            long lastPage = 0;
            for (int i = 0; i < (1 << 20); i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().bytes().writeSkip(256);
                }
                long page = (appender.lastPosition + 0xFFF) & ~0xFFF;
                boolean touch = page != lastPage && appender.wire().bytes().bytesStore().inside(page, 8);
                lastPage = page;
                if (touch != appender.microTouch())
                    assertEquals("i: " + i, touch, appender.microTouch());
                if (touch)
                    pages++;
            }
        } finally {
            System.out.println("pages = " + pages);
//        assertEquals(pagesExpected, pages);
            System.out.println("Time = " + (System.nanoTime() - start) / 1000000 / 1e3);
            IOTools.deleteDirWithFiles(path);
        }
    }
}
