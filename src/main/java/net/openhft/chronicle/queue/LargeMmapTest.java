package net.openhft.chronicle.queue;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.OnHeapBytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class LargeMmapTest {
    private static final Logger log = LoggerFactory.getLogger(LargeMmapTest.class);
    private static final boolean FORCE_SAFEPOINTS = Boolean.getBoolean("force.safepoints");
    private static final boolean SCATTER_BLOCK_SIZE = Boolean.getBoolean("scatter.block.size");
    private static final boolean SHARE_READ_QUEUES = Boolean.getBoolean("share.read.queues");
    public static final long BLOCK_SIZE = Long.getLong("block.size", 4L * 1024 * 1024 * 1024);

    public static void main(String[] args) {
        new Thread("writer") {
            @Override
            public void run() {
                try (final AffinityLock lock = AffinityLock.acquireLock(System.getProperty("writer.cpu"));
                     final SingleChronicleQueue q = createQueue()) {
                    final ExcerptAppender excerptAppender = q.acquireAppender();

                    final byte[] kilo = new byte[1024];
                    for (int i = 0; i < 1024; i++)
                        kilo[i] = (byte) i;


                    final BytesStore<?, byte[]> store = BytesStore.wrap(kilo);

                    final Integer mbps = Integer.getInteger("writer.mbps");

                    long kilosWritten = 0L;

                    long now = System.currentTimeMillis();

                    while (!Thread.currentThread().isInterrupted()) {
                        if (FORCE_SAFEPOINTS)
                            safepoint();

                        long expectedKilosWritten = (System.currentTimeMillis() - now) * 1024 / 1000 * mbps;

                        while (kilosWritten < expectedKilosWritten) {
                            kilosWritten++;

                            final long start = System.nanoTime();

                            excerptAppender.writeBytes(store);

                            final long end = System.nanoTime();

                            if (end - start > 200_000L)
                                log.warn("writer stuck for " + ((end - start) / 1_000) + "us");

                        }
                    }
                }
            }
        }.start();

        if (System.getProperty("pretoucher.cpu") != null) {
            new Thread("pretoucher") {
                @Override
                public void run() {
                    try (final AffinityLock lock = AffinityLock.acquireLock(System.getProperty("pretoucher.cpu"));
                         final SingleChronicleQueue q = createQueue()) {
                        final ExcerptAppender excerptAppender = q.acquireAppender();

                        while (!Thread.currentThread().isInterrupted()) {
                            excerptAppender.pretouch();
                        }
                    }
                }
            }.start();
        }

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
                        if (FORCE_SAFEPOINTS)
                            safepoint();

                        final long cur = System.nanoTime();

                        if (cur - prev > 200_000L)
                            log.warn("Jitterer stuck for " + ((cur - prev) / 1_000) + "us");

                        prev = cur;
                    }
                }
            }
        }.start();
    }

    private static void safepoint() {
        if (!Thread.currentThread().isAlive())
            throw new IllegalStateException("");
    }

    private static void runReader(String readerName) {
        if (System.getProperty(readerName + ".cpu") != null) {
            new Thread(readerName) {
                @Override
                public void run() {
                    try (final AffinityLock lock = AffinityLock.acquireLock(System.getProperty(readerName + ".cpu"));
                         final SingleChronicleQueue q1 = createQueue();
                         final ExcerptTailer tailer1 = q1.createTailer();
                         final SingleChronicleQueue q2 = createQueue();
                         final ExcerptTailer tailer2 = q2.createTailer()
                    ) {

                        final OnHeapBytes b = Bytes.allocateElasticOnHeap();

                        while (!Thread.currentThread().isInterrupted()) {
                            if (FORCE_SAFEPOINTS)
                                safepoint();

                            final long start = System.nanoTime();

                            tailer1.readBytes(b);
                            b.clear();

                            tailer2.readBytes(b);
                            b.clear();

                            final long end = System.nanoTime();

                            if (end - start > 200_000L)
                                log.warn(readerName + " stuck for " + ((end - start) / 1_000) + "us");
                        }
                    }
                }
            }.start();
        }
    }

    private static SingleChronicleQueue instance;

    @NotNull
    private synchronized static SingleChronicleQueue createQueue() {
        if (SHARE_READ_QUEUES) {
            if (instance == null) {
                instance = SingleChronicleQueueBuilder.builder()
                        .path("book-builder-test")
                        .sourceId(2)
                        .blockSize(SCATTER_BLOCK_SIZE ? ThreadLocalRandom.current().nextLong(BLOCK_SIZE / 2, BLOCK_SIZE) : BLOCK_SIZE)
                        .rollCycle(RollCycles.HUGE_DAILY_XSPARSE)
                        .build();
            }

            return instance;
        } else {
            return SingleChronicleQueueBuilder.builder()
                    .path("book-builder-test")
                    .sourceId(2)
                    .blockSize(SCATTER_BLOCK_SIZE ? ThreadLocalRandom.current().nextLong(BLOCK_SIZE / 2, BLOCK_SIZE) : BLOCK_SIZE)
                    .rollCycle(RollCycles.HUGE_DAILY_XSPARSE)
                    .build();
        }
    }
}
