package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;

import java.util.concurrent.locks.LockSupport;

public class BenchmarkMain {
    static int throughput = Integer.getInteger("throughput", 250); // MB/s
    static int runtime = Integer.getInteger("runtime", 300); // seconds
    static String basePath = System.getProperty("path", OS.TMP);

    public static void main(String[] args) {
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

        Thread pretoucher = new Thread(() -> {
            try (ChronicleQueue queue = ChronicleQueueBuilder.single(path).build()) {
                ExcerptAppender appender = queue.acquireAppender();
                while (!Thread.currentThread().isInterrupted()) {
                    appender.pretouch();
                    Jvm.pause(100);
                }
            }
        });
        pretoucher.setDaemon(true);
        pretoucher.start();

        Thread reader = new Thread(() -> {
            try (ChronicleQueue queue = ChronicleQueueBuilder.single(path).build()) {
                ExcerptTailer tailer = queue.createTailer();
                while (!Thread.currentThread().isInterrupted()) {
                    long transport = System.nanoTime();
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent()) {
                            continue;
                        }
                        Wire wire = dc.wire();
                        Bytes<?> bytes = wire.bytes();
                        long start = readMessage(bytes);
                        long end = System.nanoTime();
                        transportTime.sample(transport - start);
                        readTime.sample(end - transport);
                    }
                }
            }
        });
        reader.start();
        Jvm.pause(50); // give the reader time to start
        long next = System.nanoTime();
        long end = (long) (next + runtime * 1e9);
        try (ChronicleQueue queue = ChronicleQueueBuilder.single(path).build()) {
            ExcerptAppender appender = queue.acquireAppender();
            while (end > System.nanoTime()) {
                long start = System.nanoTime();
                try (DocumentContext dc = appender.writingDocument()) {
                    writeMessage(dc.wire(), messageSize);
                }
                long written = System.nanoTime();
                writeTime.sample(written - start);
                next += messageSize * 1e9 / (throughput * 1e6);
                long delay = next - System.nanoTime();
                if (delay > 0)
                    LockSupport.parkNanos(delay);
            }
        }
        while (readTime.totalCount() < writeTime.totalCount())
            Jvm.pause(50);

        pretoucher.interrupt();
        reader.interrupt();

        System.out.println("messageSize " + messageSize);
        System.out.println("messages " + writeTime.totalCount());
        System.out.println("write histogram: " + writeTime.toMicrosFormat());
        System.out.println("transport histogram: " + transportTime.toMicrosFormat());
        System.out.println("read histogram: " + readTime.toMicrosFormat());
        IOTools.deleteDirWithFiles(path, 2);
        Jvm.pause(1000);
    }

    private static long readMessage(Bytes<?> bytes) {
        long start = bytes.readLong();
        while (bytes.readRemaining() > 7)
            bytes.readLong();
        return start;
    }

    private static void writeMessage(Wire wire, int messageSize) {
        Bytes<?> bytes = wire.bytes();
        long wp = bytes.writePosition();
        for (int i = 0; i < messageSize; i += 8)
            bytes.writeLong(0);
        bytes.writeLong(wp, System.nanoTime());
    }
}
