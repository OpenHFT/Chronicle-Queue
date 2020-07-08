package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.impl.StoreFileListener;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RequiredForClient
public class AcquireReleaseTest extends ChronicleQueueTestBase {
    @Test
    public void testAcquireAndRelease() {
        File dir = DirectoryUtils.tempDir("testAcquireAndRelease");
        try {
            AtomicInteger acount = new AtomicInteger();
            AtomicInteger qcount = new AtomicInteger();
            StoreFileListener sfl = new StoreFileListener() {
                @Override
                public void onAcquired(int cycle, File file) {
//                    System.out.println("onAcquired(): " + file);
                    acount.incrementAndGet();
                }

                @Override
                public void onReleased(int cycle, File file) {
//                    System.out.println("onReleased(): " + file);
                    // TODO Auto-generated method stub
                    qcount.incrementAndGet();
                }
            };
            AtomicLong time = new AtomicLong(1000L);
            TimeProvider tp = () -> time.getAndAccumulate(1000, (x, y) -> x + y);
            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                    .testBlockSize()
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .storeFileListener(sfl)
                    .timeProvider(tp)
                    .build()) {

                int iter = 4;
                for (int i = 0; i < iter; i++) {
                    queue.acquireAppender().writeDocument(w -> {
                        w.write("a").marshallable(m -> {
                            m.write("b").text("c");
                        });
                    });
                }

                Assert.assertEquals(iter, acount.get());
                Assert.assertEquals(iter - 1, qcount.get());
            }
        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }

    @Test
    public void testReserveAndRelease() {
        File dir = DirectoryUtils.tempDir("testReserveAndRelease");

        SetTimeProvider stp = new SetTimeProvider();
        stp.currentTimeMillis(1000);
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                .testBlockSize()
                .rollCycle(RollCycles.TEST_SECONDLY)
                .timeProvider(stp)
                .build()) {
            queue.acquireAppender().writeText("Hello World");
            stp.currentTimeMillis(2000);
            queue.acquireAppender().writeText("Hello World");
            queue.createTailer().readText();
            try (ExcerptTailer tailer = queue.createTailer()) {
                tailer.readText();
                tailer.readText();
                tailer.readText();
            }
        }
    }
}
