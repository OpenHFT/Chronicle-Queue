package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
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
    public void testAccquireAndRelease() {
        File dir = DirectoryUtils.tempDir("AcquireReleaseTest");
        try {
            AtomicInteger acount = new AtomicInteger();
            AtomicInteger qcount = new AtomicInteger();
            StoreFileListener sfl = new StoreFileListener() {

                @Override
                public void onAcquired(int cycle, File file) {
                    System.out.println("onAcquired(): " + file);
                    acount.incrementAndGet();
                }

                @Override
                public void onReleased(int cycle, File file) {
                    System.out.println("onReleased(): " + file);
                    // TODO Auto-generated method stub
                    qcount.incrementAndGet();
                }
            };
            AtomicLong time = new AtomicLong(1000l);
            TimeProvider tp = () -> time.getAndAccumulate(1000, (x, y) -> x + y);
            ChronicleQueue queue = ChronicleQueue.singleBuilder(dir)
                    .testBlockSize()
                    .rollCycle(RollCycles.TEST_SECONDLY)
                    .storeFileListener(sfl)
                    .timeProvider(tp)
                    .build();
            for (int i = 0; i < 10; i++) {
                queue.acquireAppender().writeDocument(w -> {
                    w.write("a").marshallable(m -> {
                        m.write("b").text("c");
                    });
                });
            }
            Assert.assertEquals(10, acount.get());
            Assert.assertEquals(9, qcount.get());
            queue.close();
        } finally {
            try {
                IOTools.deleteDirWithFiles(dir, 2);
            } catch (IORuntimeException e) {
                // ignored
            }
        }
    }
}
