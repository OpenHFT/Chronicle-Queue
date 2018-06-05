package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Memory;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.LockSupport;

public class BenchmarkMain {
    static int throughput = Integer.getInteger("throughput", 250); // MB/s
    static int runtime = Integer.getInteger("runtime", 300); // seconds
    static String basePath = System.getProperty("path", OS.TMP);

    static volatile long writeCount;

    public static void main(String[] args) {
        System.out.println(
                "-Dthroughput=" + throughput
                        + " -Druntime=" + runtime
                        + " -Dpath=" + basePath);
        MappedFile.warmup();
        System.out.println("Warming up");
        benchmark(128);
        System.out.println("Warmed up");
        for (int size = 64; size <= 16 << 20; size *= 4) {
            benchmark(size);
        }
    }

    static void benchmark(int messageSize) {
        Histogram writeTime = new Histogram(32, 7);
        Histogram transportTime = new Histogram(32, 7);
        Histogram readTime = new Histogram(32, 7);
        String path = basePath + "/test-q-" + messageSize;

        ChronicleQueue queue = createQueue(path);

        Thread pretoucher = new Thread(() -> {
            ExcerptAppender appender = queue.acquireAppender();
            Thread thread = Thread.currentThread();
            while (!thread.isInterrupted()) {
                appender.pretouch();
                Jvm.pause(10);
            }
        });
        pretoucher.setDaemon(true);
        pretoucher.start();

        Thread reader = new Thread(() -> {
            try (ChronicleQueue queue2 = createQueue(path)) {
                ExcerptTailer tailer = queue2.createTailer().toEnd();
                Thread thread = Thread.currentThread();
                while (!thread.isInterrupted()) {
                    Jvm.safepoint();
                    try (DocumentContext dc = tailer.readingDocument()) {
                        Jvm.safepoint();
                        if (!dc.isPresent()) {
                            continue;
                        }
                        long transport = System.nanoTime();
                        Jvm.safepoint();
                        Wire wire = dc.wire();
                        Bytes<?> bytes = wire.bytes();
                        long start = readMessage(bytes);
                        long end = System.nanoTime();
                        transportTime.sample(transport - start);
                        readTime.sample(end - transport);
                    }
                    Jvm.safepoint();
                }
            }
        });
        reader.start();
        Jvm.pause(250); // give the reader time to start
        long next = System.nanoTime();
        long end = (long) (next + runtime * 1e9);

//        Thread writer = Thread.currentThread();

/*
        Thread monitor = new Thread(() -> {
            Thread thread = Thread.currentThread();
            while (!thread.isInterrupted()) {
                long diff = writeTime.totalCount() - readTime.totalCount();
                if (diff > 100) {
                    System.out.println("diff=" + diff);
                    StringBuilder sb = new StringBuilder();
                    sb.append("Reader: profile of the thread");
                    Jvm.trimStackTrace(sb, reader.getStackTrace());
                    System.out.println(sb);
                    Jvm.pause(50);
                }
                Jvm.pause(5);
            }
        });
        monitor.setDaemon(true);
        monitor.start();
*/

        ExcerptAppender appender = queue.acquireAppender();
        while (end > System.nanoTime()) {
            long start = System.nanoTime();
            try (DocumentContext dc = appender.writingDocument()) {
                writeMessage(dc.wire(), messageSize);
            }
            long written = System.nanoTime();
            long time = written - start;
//                System.out.println(time);
            writeTime.sample(time);

            long diff = writeTime.totalCount() - readTime.totalCount();
            if (diff > 2) {
                System.out.println("diff=" + diff);
                StringBuilder sb = new StringBuilder();
                sb.append("Reader: profile of the thread");
                Jvm.trimStackTrace(sb, reader.getStackTrace());
                System.out.println(sb);
            }

            next += messageSize * 1e9 / (throughput * 1e6);
            long delay = next - System.nanoTime();
            if (delay > 0)
                LockSupport.parkNanos(delay);
        }

        while (readTime.totalCount() < writeTime.totalCount())
            Jvm.pause(50);

        pretoucher.interrupt();
        reader.interrupt();
//        monitor.interrupt();

        System.out.println("messageSize " + messageSize);
        System.out.println("messages " + writeTime.totalCount());
        System.out.println("write histogram: " + writeTime.toMicrosFormat());
        System.out.println("transport histogram: " + transportTime.toMicrosFormat());
        System.out.println("read histogram: " + readTime.toMicrosFormat());
        IOTools.deleteDirWithFiles(path, 2);
        Jvm.pause(1000);
    }

    @NotNull
    private static SingleChronicleQueue createQueue(String path) {
        return ChronicleQueueBuilder.single(path)
                .blockSize(1 << 30)
                .build();
    }

    private static long readMessage(Bytes<?> bytes) {
        Jvm.safepoint();
        long start = bytes.readLong();
        if (true) {
            long rp = bytes.readPosition();
            long rl = bytes.readLimit();
            long addr = bytes.addressForRead(rp);
            long addrEnd = bytes.addressForRead(rl);
            Memory memory = OS.memory();
            for (addr += 8; addr + 7 < addrEnd; addr += 8)
                memory.readLong(addr);
        } else {
            while (bytes.readRemaining() > 7)
                bytes.readLong();
        }
        Jvm.safepoint();
        return start;
    }

    private static void writeMessage(Wire wire, int messageSize) {
        Bytes<?> bytes = wire.bytes();
        long wp = bytes.writePosition();
        // TODO Optimise wire to give similar performance.
        if (false) {
            for (int i = 0; i < messageSize; i += 8)
                bytes.writeLong(0L);

        } else {
            long addr = bytes.addressForWrite(wp);
            Memory memory = OS.memory();
            for (int i = 0; i < messageSize; i += 16) {
                memory.writeLong(addr + i, 0L);
                memory.writeLong(addr + i + 8, 0L);
            }

            bytes.writeSkip(messageSize);
        }
        bytes.writeLong(wp, System.nanoTime());
    }
}
