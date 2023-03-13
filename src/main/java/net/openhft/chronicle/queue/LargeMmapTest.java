package net.openhft.chronicle.queue;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.OnHeapBytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LargeMmapTest {
    private static final Logger log = LoggerFactory.getLogger(LargeMmapTest.class);

    public static void main(String[] args) {
        new Thread("writer") {
            @Override
            public void run() {
                try (final AffinityLock lock = AffinityLock.acquireLock(System.getProperty("writer.cpu"));
                     final SingleChronicleQueue q = SingleChronicleQueueBuilder.builder()
                             .path("book-builder-test")
                             .sourceId(2)
                             .blockSize(4L * 1024 * 1024 * 1024)
                             .rollCycle(RollCycles.HUGE_DAILY_XSPARSE)
                             .build()) {
                    final ExcerptAppender excerptAppender = q.acquireAppender();

                    final byte[] kilo = new byte[1024];
                    for (int i = 0; i < 1024; i++)
                        kilo[i] = (byte) i;


                    final BytesStore<?, byte[]> store = BytesStore.wrap(kilo);

                    final Integer mbps = Integer.getInteger("writer.mbps");

                    long kilosWritten = 0L;

                    long now = System.currentTimeMillis();

                    while (!Thread.currentThread().isInterrupted()) {
                        long expectedKilosWritten = (System.currentTimeMillis() - now) * 1024 / 1000 * mbps;

                        while (kilosWritten < expectedKilosWritten) {
                            kilosWritten++;

                            excerptAppender.writeBytes(store);
                        }
                    }
                }
            }
        }.start();

        runReader("reader");
        runReader("reader2");
        runReader("reader3");
        runReader("reader4");
        runReader("reader5");

        new Thread("jitterer") {
            @Override
            public void run() {
                try (final AffinityLock lock = AffinityLock.acquireLock(System.getProperty("jitterer.cpu"))) {

                    long prev = System.nanoTime();
                    while (!Thread.currentThread().isInterrupted()) {
                        final long cur = System.nanoTime();

                        if (cur - prev > 200_000L)
                            log.warn("Jitterer stuck for " + ((cur - prev) / 1_000) + "us");

                        prev = cur;
                    }
                }
            }
        }.start();
    }

    private static void runReader(String readerName) {
        if (System.getProperty(readerName + ".cpu") != null) {
            new Thread(readerName) {
                @Override
                public void run() {
                    try (final AffinityLock lock = AffinityLock.acquireLock(System.getProperty(readerName + ".cpu"));
                         final SingleChronicleQueue q1 = SingleChronicleQueueBuilder.builder()
                                 .path("book-builder-test")
                                 .sourceId(2)
                                 .blockSize(4L * 1024 * 1024 * 1024)
                                 .rollCycle(RollCycles.HUGE_DAILY_XSPARSE)
                                 .build();
                         final ExcerptTailer tailer1 = q1.createTailer();
                         final SingleChronicleQueue q2 = SingleChronicleQueueBuilder.builder()
                                 .path("book-builder-test")
                                 .sourceId(2)
                                 .blockSize(4L * 1024 * 1024 * 1024)
                                 .rollCycle(RollCycles.HUGE_DAILY_XSPARSE)
                                 .build();
                         final ExcerptTailer tailer2 = q2.createTailer()
                    ) {

                        final OnHeapBytes b = Bytes.allocateElasticOnHeap();

                        while (!Thread.currentThread().isInterrupted()) {
                            tailer1.readBytes(b);
                            b.clear();

                            tailer2.readBytes(b);
                            b.clear();
                        }
                    }
                }
            }.start();
        }
    }
}
