package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

public class ThroughputPerfMain {
    static final int time = Integer.getInteger("time", 60);
    static final int size = Integer.getInteger("size", 48);
    static final String path = System.getProperty("path", OS.TMP);

    public static void main(String[] args) {
        String base = path + "/delete-" + System.nanoTime() + ".me";
        long start = System.nanoTime();
        long count = 0;
        NativeBytesStore nbs = NativeBytesStore.nativeStoreWithFixedCapacity(size);

        long blockSize = 4L << 30;
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(base).blockSize(blockSize).build()) {
            ExcerptAppender appender = q.acquireAppender();
            do {
                int batch = Math.max(1, (128 << 10) / size);
                for (int i = 0; i < batch; i++) {
                    try (DocumentContext dc = appender.writingDocument()) {
                        dc.wire().bytes()
                                .write(nbs);
                    }
                }
                count += batch;
            } while (start + time * 1e9 > System.nanoTime());
        }
        nbs.release();
        long mid = System.nanoTime();
        long time1 = mid - start;

        Bytes bytes = Bytes.allocateElasticDirect(64);
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(base).blockSize(blockSize).build()) {
            ExcerptTailer tailer = q.createTailer();
            for (long i = 0; i < count; i++) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    bytes.clear();
                    bytes.write(dc.wire().bytes());
                }
            }
        }
        bytes.release();
        long end = System.nanoTime();
        long time2 = end - mid;

        System.out.printf("Writing %,d messages took %.3f seconds, at a rate of %,d per second%n",
                count, time1 / 1e9, (long) (1e9 * count / time1));
        System.out.printf("Reading %,d messages took %.3f seconds, at a rate of %,d per second%n",
                count, time2 / 1e9, (long) (1e9 * count / time2));

        System.gc(); // make sure its cleaned up for windows to delete.
        IOTools.deleteDirWithFiles(base, 2);
    }
}
