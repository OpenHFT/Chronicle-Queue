package net.openhft.chronicle.queue.benchmark;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.UnsafeMemory;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static net.openhft.chronicle.queue.benchmark.Main.*;

/* Ubuntu i7-10710U CPU @ 1.10GHz
Writing 23,068,441 messages took 5.076 seconds, at a rate of 4,544,000 per second
Reading 23,068,441 messages took 2.728 seconds, at a rate of 8,456,000 per second
 */
public class ThroughputMain {

    public static void main(String[] args) {
        System.out.println("Testing with " +
                "-Dtime=" + time + " " +
                "-Dthreads=" + threads + " " +
                "-Dsize=" + size + " " +
                "-Dpath=" + path + " " +
                "-DfullWrite=" + fullWrite);

        long start = System.nanoTime();
        String base = path + "/delete-" + Time.uniqueId() + ".me.";

        long blockSize = OS.is64Bit()
                ? OS.isLinux()
                ? 4L << 30
                : 1L << 30
                : 256L << 20;

        AtomicLong count = new AtomicLong();
        IntStream.range(0, threads).parallel().forEach(i -> {
            long count2 = 0;
            BytesStore<?, Void> nbs = BytesStore.nativeStoreWithFixedCapacity(size);

            try (ChronicleQueue q = ChronicleQueue.singleBuilder(base + i)
                    .rollCycle(RollCycles.LARGE_HOURLY_XSPARSE)
                    .blockSize(blockSize)
                    .build()) {

                ExcerptAppender appender = q.acquireAppender();
                long lastIndex = -1;
                do {
                    int defaultIndexSpacing = q.rollCycle().defaultIndexSpacing();
                    Wire wire = appender.wire();
                    int writeCount = (int) (defaultIndexSpacing - (lastIndex & (defaultIndexSpacing - 1)) - 1);
                    if (!fullWrite && wire != null && writeCount > 0) {
                        MappedBytes bytes = (MappedBytes) wire.bytes();
                        long address = bytes.addressForWrite(bytes.writePosition());
                        long bstart = bytes.start();
                        long bcap = bytes.realCapacity();
                        long canWrite = bcap - (bytes.writePosition() - bstart);
                        long lengthCount = writeMessages(address, canWrite, writeCount, nbs);
                        bytes.writeSkip((int) lengthCount);
                        lastIndex += lengthCount >> 32;
                        count2 += lengthCount >> 32;

                    } else {
                        try (DocumentContext dc = appender.writingDocument()) {
                            Wire wire2 = dc.wire();
                            wire2.bytes().write(nbs);
                            addToEndOfCache(wire2);
                        }
                        lastIndex = appender.lastIndexAppended();
                        count2++;
                    }
                } while (start + time * 1e9 > System.nanoTime());
            }
            // System.out.println("... All data written, now reading ...");
            nbs.releaseLast();
            count.addAndGet(count2);
        });
        long time1 = System.nanoTime() - start;
        Jvm.pause(1000);
        System.gc();
        long mid = System.nanoTime();
        IntStream.range(0, threads).parallel().forEach(i -> {

            Bytes<?> bytes = Bytes.allocateElasticDirect(64);
            try (ChronicleQueue q = ChronicleQueue.singleBuilder(base + i)
                    .rollCycle(RollCycles.LARGE_HOURLY_XSPARSE)
                    .blockSize(blockSize)
                    .build()) {
                ExcerptTailer tailer = q.createTailer();
                for (; ; ) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent()) break;

                        bytes.clear();
                        bytes.write(dc.wire().bytes());
                    }
                }
            }
            bytes.releaseLast();
        });
        long end = System.nanoTime();
        long time2 = end - mid;

        System.out.printf("Writing %,d messages took %.3f seconds, at a rate of %,d per second%n",
                count.longValue(), time1 / 1e9, 1000 * (long) (1e6 * count.get() / time1));
        System.out.printf("Reading %,d messages took %.3f seconds, at a rate of %,d per second%n",
                count.longValue(), time2 / 1e9, 1000 * (long) (1e6 * count.get() / time2));

        Jvm.pause(200);
        System.gc(); // make sure its cleaned up for windows to delete.
        IntStream.range(0, threads).forEach(i ->
                IOTools.deleteDirWithFiles(base + i, 2));
    }

    static void addToEndOfCache(Wire wire2) {
        Bytes<?> bytes = wire2.bytes();
        long addr = bytes.addressForWrite(bytes.writePosition());
        int pad = (int) (64 - (addr & 63));
        if (pad < 64)
            wire2.addPadding(pad);
    }

    @SuppressWarnings("restriction")
    private static long writeMessages(long address, long canWrite, int writeCount, BytesStore nbs) {
        long length = 0;
        long count = 0;
        // writeCount = writeCount == 1 ? 1 : ThreadLocalRandom.current().nextInt(writeCount-1)+1;
        long fromAddress = nbs.addressForRead(0);
        while (writeCount > count && length + 4 + size <= canWrite) {
            UnsafeMemory.UNSAFE.copyMemory(fromAddress, address + 4, size);
            UnsafeMemory.UNSAFE.putOrderedInt(null, address, size);
            address += 4 + size;
            length += 4 + size;
            count++;
        }
        // System.out.println("w "+count+" "+length);
        return (count << 32) | length;
    }
}
