/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.PageUtil;
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

import static net.openhft.chronicle.queue.rollcycles.LargeRollCycles.HUGE_DAILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class MicroToucherTest extends QueueTestCommon {

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @Test
    public void touchPageSparse() {
        assumeTrue(OS.isLinux());
        touchPage(b -> b.useSparseFiles(true).rollCycle(HUGE_DAILY), 66561);
    }

    @Test
    public void touchPageTestBlockSize() {
        touchPage(b -> b.blockSize(64 << 20), 66561);
    }

    public void touchPage(Consumer<SingleChronicleQueueBuilder> configure, int pagesExpected) {
        long start = System.nanoTime();
        String path = OS.getTarget() + "/touchPage-" + System.nanoTime();
        int pages = 0;
        final SingleChronicleQueueBuilder builder = ChronicleQueue.singleBuilder(path);
        configure.accept(builder);
        String queuePath = builder.path().getAbsolutePath();
        if (PageUtil.isHugePage(queuePath)) {
            // Set the sparse capacity to a sufficiently large multiple of the page size
            builder.sparseCapacity(PageUtil.getPageSize(queuePath) * 200);
        }
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