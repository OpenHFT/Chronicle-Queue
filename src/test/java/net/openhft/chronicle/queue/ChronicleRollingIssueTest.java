package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ChronicleRollingIssueTest {

    @Test(timeout = 20000)
    @Ignore("TODO FIX Reset write position")
    public void test() throws Exception {
        int threads = 64;
        int messages = 100;

        String path = OS.TARGET + "/ChronicleRollingIssueTest-" + System.nanoTime();
        AtomicInteger count = new AtomicInteger();

        Runnable appendRunnable = () -> {
            try (final ChronicleQueue writeQueue = ChronicleQueueBuilder
                    .single(path)
                    .rollCycle(RollCycles.TEST_SECONDLY).build()) {
                for (int i = 0; i < messages; i++) {
                    ExcerptAppender appender = writeQueue.createAppender();
                    Map<String, Object> map = new HashMap<>();
                    map.put("k1", "v1");
                    appender.writeMap(map);
                    count.incrementAndGet();
                    Jvm.pause(100);
                }
            }
        };

        for (int i = 0; i < threads; i++) {
            new Thread(appendRunnable, "appender-" + i).start();
        }
        try (final ChronicleQueue readQueue = ChronicleQueueBuilder
                .single(path)
                .rollCycle(RollCycles.TEST_SECONDLY).build()) {
            ExcerptTailer tailer = readQueue.createTailer();
            int count2 = 0, last = 0;
            while (count2 < threads * messages) {
                Map<String, Object> map = tailer.readMap();
                if (map != null)
                    count2++;
                final int i = count.get();
                if (i % 200 == 0 && i > last)
                    System.out.println("Wrote: " + count
                            + " read: " + count2
                            + " index: " + Long.toHexString(tailer.index()));
                last = i;
            }
        } finally {
            try {
                IOTools.deleteDirWithFiles(path, 2);
            } catch (IORuntimeException todoFixOnWindows) {

            }
        }
    }
}