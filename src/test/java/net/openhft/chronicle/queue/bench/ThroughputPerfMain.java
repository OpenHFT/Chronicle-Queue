package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.UnsafeMemory;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;

public class ThroughputPerfMain {
    static final int time = Integer.getInteger("time", 20);
    static final int size = Integer.getInteger("size", 48);
    static final String path = System.getProperty("path", OS.TMP);

    static NativeBytesStore nbs;

    public static void main(String[] args) {
        String base = path + "/delete-" + System.nanoTime() + ".me";
        long start = System.nanoTime();
        long count = 0;
        nbs = NativeBytesStore.nativeStoreWithFixedCapacity(size);

        long blockSize = 4L << 30;
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(base)
                .rollCycle(RollCycles.LARGE_HOURLY_SPARSE)
                .blockSize(blockSize)
                .build()) {
            ExcerptAppender appender = q.acquireAppender();
            long lastIndex = -1;
            do {
                int batch = Math.max(1, (128 << 10) / size);
                int defaultIndexSpacing = q.rollCycle().defaultIndexSpacing();
                for (int i = 0; i < batch; i++) {
                    Wire wire = appender.wire();
                    int writeCount = (int) (defaultIndexSpacing - (lastIndex & (defaultIndexSpacing - 1)) - 1);
                    if (wire != null && writeCount > 0) {
                        MappedBytes bytes = (MappedBytes) wire.bytes();
                        long address = bytes.addressForWrite(bytes.writePosition());
                        long bstart = bytes.start();
                        long bcap = bytes.realCapacity();
                        long canWrite = bcap - (bytes.writePosition() - bstart);
                        long lengthCount = writeMessages(address, canWrite, writeCount);
                        bytes.writeSkip((int) lengthCount);
                        lastIndex += lengthCount >> 32;
                        count += lengthCount >> 32;

                    } else {
                        try (DocumentContext dc = appender.writingDocument()) {
                            dc.wire().bytes()
                                    .write(nbs);
                        }
                        lastIndex = appender.lastIndexAppended();
                        count++;
                    }
                }
            } while (start + time * 1e9 > System.nanoTime());
        }
        nbs.release();
        long mid = System.nanoTime();
        long time1 = mid - start;

        Bytes bytes = Bytes.allocateElasticDirect(64);
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(base)
                .rollCycle(RollCycles.LARGE_HOURLY_SPARSE)
                .blockSize(blockSize)
                .build()) {
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

    private static long writeMessages(long address, long canWrite, int writeCount) {
        long length = 0;
        long count = 0;
//        writeCount = writeCount == 1 ? 1 : ThreadLocalRandom.current().nextInt(writeCount-1)+1;
        long fromAddress = nbs.addressForRead(0);
        while (writeCount > count && length + 4 + size <= canWrite) {
            UnsafeMemory.UNSAFE.copyMemory(fromAddress, address + 4, size);
            UnsafeMemory.UNSAFE.putOrderedInt(null, address, size);
            address += 4 + size;
            length += 4 + size;
            count++;
        }
//        System.out.println("w "+count+" "+length);
        return (count << 32) | length;
    }
}
