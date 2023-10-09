package net.openhft.chronicle.queue.bench.multiprocess;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static net.openhft.chronicle.queue.rollcycles.LargeRollCycles.LARGE_DAILY;

public class ProducerService {

    private static SingleChronicleQueue queue;
    private static final Datum datum = new Datum();

    static {
        System.setProperty("disable.thread.safety", "true");
        System.setProperty("jvm.resource.tracing", "false");
        System.setProperty("check.thread.safety", "false");
    }

    public static void main(String[] args) {
        queue = single("replica").rollCycle(LARGE_DAILY).doubleBuffer(false).build();
        run(datum);
    }

    private static void run(Datum datum21) {
        try (final AffinityLock ignored = AffinityLock.acquireCore();
             final ExcerptAppender app = queue.createAppender()) {
            int counter = 1000000;
            while (counter > 0) {
                counter--;
                final long start = ClockUtil.getNanoTime();
                datum21.ts = start;
                datum21.username = "" + start;
                try (DocumentContext dc = app.writingDocument()) {
                    dc.wire().write("datum").marshallable(datum21);
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            queue.close();
        }
    }
}
