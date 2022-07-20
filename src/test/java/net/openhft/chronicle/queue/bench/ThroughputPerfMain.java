package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;

/*
Ryzen 9 5950X with Corsair MP600 PRO XT
-Dtime=30 -Dsize=1000000 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 44,749 messages took 30.001 seconds, at a rate of 1,491 per second
Reading 44,749 messages took 14.461 seconds, at a rate of 3,094 per second

-Dtime=30 -Dsize=100000 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 428,833 messages took 30.001 seconds, at a rate of 14,294 per second
Reading 428,833 messages took 14.407 seconds, at a rate of 29,764 per second

-Dtime=30 -Dsize=10000 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 4,314,082 messages took 30.001 seconds, at a rate of 143,798 per second
Reading 4,314,082 messages took 20.516 seconds, at a rate of 210,279 per second

-Dtime=30 -Dsize=1000 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 41,240,632 messages took 30.001 seconds, at a rate of 1,374,647 per second
Reading 41,240,632 messages took 17.056 seconds, at a rate of 2,418,022 per second

-Dtime=30 -Dsize=300 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 107,668,614 messages took 30.001 seconds, at a rate of 3,588,840 per second
Reading 107,668,614 messages took 5.235 seconds, at a rate of 20,567,778 per second

-Dtime=30 -Dsize=100 -Dpath=/data/tmp - DblockSizeMB=524288
Writing 188,511,678 messages took 30.001 seconds, at a rate of 6,283,489 per second
Reading 188,511,678 messages took 6.509 seconds, at a rate of 28,959,528 per second
 */
public class ThroughputPerfMain {
    private static final int TIME = Integer.getInteger("time", 30);
    private static final int SIZE = Integer.getInteger("size", 40);
    private static final String PATH = System.getProperty("path", OS.TMP);
    private static final long blockSizeMB = Long.getLong("blockSizeMB", OS.isSparseFileSupported() ? 512L << 10 : 256L);

    private static BytesStore nbs;

    public static void main(String[] args) {
        String base = PATH + "/delete-" + Time.uniqueId() + ".me";
        long start = System.nanoTime();
        long count = 0;
        nbs = BytesStore.nativeStoreWithFixedCapacity(SIZE);

        try (ChronicleQueue q = ChronicleQueue.singleBuilder(base)
                .rollCycle(RollCycles.LARGE_HOURLY_XSPARSE)
                .blockSize(blockSizeMB << 20)
                .build()) {

            ExcerptAppender appender = q.acquireAppender();
            do {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().bytes().write(nbs);
                }
                count++;
            } while (start + TIME * 1e9 > System.nanoTime());
        }

        nbs.releaseLast();
        long mid = System.nanoTime();
        long time1 = mid - start;

        Bytes<?> bytes = Bytes.allocateElasticDirect(64);
        try (ChronicleQueue q = ChronicleQueue.singleBuilder(base)
                .rollCycle(RollCycles.LARGE_HOURLY_XSPARSE)
                .blockSize(blockSizeMB << 20)
                .build()) {
            ExcerptTailer tailer = q.createTailer();
            for (long i = 0; i < count; i++) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    bytes.clear();
                    bytes.write(dc.wire().bytes());
                }
            }
        }
        bytes.releaseLast();
        long end = System.nanoTime();
        long time2 = end - mid;

        System.out.println("-Dtime=" + TIME +
                " -Dsize=" + SIZE +
                " -Dpath=" + PATH +
                " - DblockSizeMB=" + blockSizeMB);
        System.out.printf("Writing %,d messages took %.3f seconds, at a rate of %,d per second%n",
                count, time1 / 1e9, (long) (1e9 * count / time1));
        System.out.printf("Reading %,d messages took %.3f seconds, at a rate of %,d per second%n",
                count, time2 / 1e9, (long) (1e9 * count / time2));

        BackgroundResourceReleaser.releasePendingResources();
        System.gc(); // make sure its cleaned up for windows to delete.
        IOTools.deleteDirWithFiles(base, 2);
    }
}
