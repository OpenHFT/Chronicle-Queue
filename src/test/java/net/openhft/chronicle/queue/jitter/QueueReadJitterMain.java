package net.openhft.chronicle.queue.jitter;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.concurrent.atomic.AtomicLong;

public class QueueReadJitterMain {
    public static final String PROFILE_OF_THE_THREAD = "profile of the thread";

    static int runTime = Integer.getInteger("runTime", 600); // seconds
    static int size = Integer.getInteger("size", 128); // bytes
    static int sampleTime = Integer.getInteger("sampleTime", 30); // micro-seconds
    static volatile boolean running = true;

    static {
        System.setProperty("jvm.safepoint.enabled", "true");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
    }

    public static void main(String[] args) {
        new QueueReadJitterMain().run();
    }

    protected void run() {
        MappedFile.warmup();

        String path = "test-q-" + Time.uniqueId();
//        System.out.println("Writing to " + path);
        AtomicLong lastRead = new AtomicLong();

        Thread reader = new Thread(() -> {
            try (ChronicleQueue q = createQueue(path)) {
                ExcerptTailer tailer = q.createTailer().toEnd();
                while (running) {
                    Jvm.safepoint();
                    try (DocumentContext dc = tailer.readingDocument(false)) {
                        if (!dc.isPresent()) {
                            Jvm.safepoint();
                            continue;
                        }
                        Jvm.safepoint();
                        Bytes<?> bytes = dc.wire().bytes();
                        long count = bytes.readLong();
                        while (bytes.readRemaining() > 7)
                            bytes.readLong();
                        lastRead.set(count);
                        Jvm.safepoint();
                    }
                    Jvm.safepoint();
                }
            }
        });
        reader.setDaemon(true);
        reader.start();
        Jvm.pause(100); // give it time to start

        long count = 0;
        try (ChronicleQueue q = createQueue(path)) {
            ExcerptAppender appender = q.acquireAppender();
            long start0 = System.currentTimeMillis();
            do {
                try (DocumentContext dc = appender.writingDocument()) {
                    Bytes<?> bytes = dc.wire().bytes();
                    bytes.writeLong(++count);
                    for (int i = 8; i < size; i += 8)
                        bytes.writeLong(0);
                }
                long start1 = System.nanoTime();
                do {
                    Thread.yield();
                } while (System.nanoTime() < start1 + sampleTime * 1000);
                long time1 = System.nanoTime() - start1;
                if (time1 > sampleTime * 1000 * 10) {
                    System.out.println("Time paused = " + time1 / 1000 + " us");
                }
                if (lastRead.get() != count) {
                    StackTraceElement[] stes = reader.getStackTrace();
                    StringBuilder sb = new StringBuilder();
                    sb.append(PROFILE_OF_THE_THREAD);
                    Jvm.trimStackTrace(sb, stes);
                    System.out.println(sb);
                }
            } while (System.currentTimeMillis() < start0 + runTime * 1_000);
        }
        running = false;
        IOTools.deleteDirWithFiles(path, 2);
    }

    protected ChronicleQueue createQueue(String path) {
        return SingleChronicleQueueBuilder.single(path).blockSize(1 << 20).build();
    }
}
