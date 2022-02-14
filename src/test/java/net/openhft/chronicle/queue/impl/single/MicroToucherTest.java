package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class MicroToucherTest extends QueueTestCommon {

    @Test
    public void touchPageSparse() {
        assumeTrue(OS.isLinux());
        touchPage(b -> b.useSparseFiles(true).rollCycle(RollCycles.HUGE_DAILY), 66561);
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
        try (ChronicleQueue q = builder.build();
             final StoreAppender appender = (StoreAppender) q.acquireAppender()) {

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
        }
        System.out.println("pages = " + pages);
//        assertEquals(pagesExpected, pages);
        System.out.println("Time = " + (System.nanoTime() - start) / 1000000 / 1e3);
        IOTools.deleteDirWithFiles(path);
    }
}