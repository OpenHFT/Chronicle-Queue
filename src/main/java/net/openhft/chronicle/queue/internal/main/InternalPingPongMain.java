package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;

public final class InternalPingPongMain {
    //    static int throughput = Integer.getInteger("throughput", 250); // MB/s
    static int runtime = Integer.getInteger("runtime", 30); // seconds
    static String basePath = System.getProperty("path", OS.TMP);
    static volatile long writeTime = 0;
    static volatile int writeCount = 0;
    static volatile int readCount = 0;
    static volatile boolean running = true;

    static {
        System.setProperty("jvm.safepoint.enabled", "true");
    }

    public static void main(String[] args) {
        System.out.println(
//                "-Dthroughput=" + throughput
                " -Druntime=" + runtime
                        + " -Dpath=" + basePath);
        MappedFile.warmup();

        pingPong(64);
    }

    static void pingPong(int size) {
        String path = InternalPingPongMain.basePath + "/test-q-" + Time.uniqueId();
        Histogram readDelay = new Histogram();
        Histogram readDelay2 = new Histogram();
        try (ChronicleQueue queue = createQueue(path)) {

            Thread reader = new Thread(() -> {
                ExcerptTailer tailer = queue.createTailer();
                while (running) {
                    //noinspection StatementWithEmptyBody
                    while (readCount == writeCount) ;

                    long wakeTime = System.nanoTime();
                    while (running) {
                        try (DocumentContext dc = tailer.readingDocument(true)) {
                            if (!dc.isPresent())
                                continue;
                        }
                        break;
                    }
                    long delay = wakeTime - writeTime;
                    long time = System.nanoTime() - wakeTime;
                    readDelay2.sample(time);
                    readDelay.sample(delay);
                    if (time + delay > 20_000)
                        System.out.println("td " + delay + " + " + time);
                    if (readCount == 100000) {
                        System.out.println("reset");
                        readDelay.reset();
                        readDelay2.reset();
                    }
                    readCount++;
                }
            });
            reader.setDaemon(true);
            reader.start();
            Jvm.pause(100);

            long finish = System.currentTimeMillis() + runtime * 1000;
            ExcerptAppender appender = queue.acquireAppender();
            while (System.currentTimeMillis() < finish) {
                if (readCount < writeCount) {
                    Thread.yield();
                    continue;
                }
                try (DocumentContext dc = appender.writingDocument(false)) {
                    dc.wire().bytes().writeSkip(size);
                }
                writeCount++;
                writeTime = System.nanoTime();
            }
            running = false;
        }
        System.out.println("read delay: " + readDelay.toMicrosFormat());
        System.out.println("read delay2: " + readDelay2.toMicrosFormat());
        IOTools.deleteDirWithFiles(path, 2);
    }

    @NotNull
    private static ChronicleQueue createQueue(String path) {
        return ChronicleQueue.single(path);
    }

}
