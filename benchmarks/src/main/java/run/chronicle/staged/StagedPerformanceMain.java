package run.chronicle.staged;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/*
-Dstages=10 on an 8 core desktop with HT & NVMe drive
==== Throughput 500k/s ====
Producer wrote 600000000 messages in 1200.000302 seconds
End to end 99%ile latency 0.005 ms 50/90 99/99.9 99.99/99.999 99.9999/worst was 3.00 / 3.29  4.78 / 1,530  3,790 / 48,890  56,750 / 57,540

-Dstages=7 on an 8 core desktop with an NVMe drive
==== Throughput 500k/s ====
Producer wrote 600000000 messages in 1200.000289 seconds
End to end 99%ile latency 0.003 ms 50/90 99/99.9 99.99/99.999 99.9999/worst was 2.15 / 2.38  2.79 / 920  2,840 / 22,480  30,740 / 31,520

==== Throughput 1000k/s ====
Producer wrote 120000000 messages in 120.000231 seconds
End to end 99%ile latency 0.007 ms 50/90 99/99.9 99.99/99.999 99.9999/worst was 2.14 / 2.41  6.90 / 1,540  6,140 / 13,400  13,800 / 14,390

 */
public class StagedPerformanceMain {
    static final String PATH = System.getProperty("path", OS.TMP);
    static int STAGES = Integer.getInteger("stages", Math.min(Runtime.getRuntime().availableProcessors() / 2 - 1, 10));
    static int THROUGHPUT = Integer.getInteger("throughput", 0);
    static int COUNT = Integer.getInteger("count", 30 * THROUGHPUT);
    static int REPORT_INTERVAL;
    static String DIR;
    static boolean WARMUP;
    private static ChronicleQueue q0;

    private static int interval() {
        return 1_000_000_000 / THROUGHPUT;
    }

    public static void main(String... args) {
        System.out.println("Testing -Dstages=" + STAGES + " -Dpath=" + PATH);
        Jvm.setExceptionHandlers(Jvm.error(), Jvm.warn(), null, null);
        MappedFile.warmup();
        runBenchmark(2_000_000, 2_000, true);
        System.out.println("Warmup complete");

        if (THROUGHPUT > 0 && COUNT > 0 && STAGES > 0) {
            runBenchmark(COUNT, interval(), false);
            return;
        }
        for (int factor : new int[]{2, 12, 120, 1200}) {
            int t = 500;
            THROUGHPUT = t * 1000;
            COUNT = THROUGHPUT * factor;
            System.out.println("\n==== Throughput " + t + "k/s ====");
            runBenchmark(COUNT, interval(), false);
        }
    }

    private static void runBenchmark(int count, int interval, boolean warmup) {
        String run = Long.toString(System.nanoTime(), 36);
        WARMUP = warmup;
        DIR = PATH + "/run-" + run;

        new File(DIR).mkdirs();
        REPORT_INTERVAL = (int) (60e9 / interval); // every 60 seconds.

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(DIR + "/data").build()) {
            q0 = queue;

            List<Runnable> runnables = new ArrayList<>();
            runnables.add(() -> producer(count, interval));
            runnables.add(() -> firstStage());
            for (int s = 2; s <= STAGES; s++) {
                int finalS = s;
                runnables.add(() -> runStage(finalS));
            }
            runnables.stream().parallel().forEach(Runnable::run);

            Histogram latencies = new Histogram();
            reportLatenciesForStage(STAGES, latencies);

        }
        IOTools.deleteDirWithFiles(DIR, 3);
    }

    public static void producer(int count, int intervalNS) {
        IFacadeAll datum = Values.newNativeReference(IFacadeAll.class);
        long datumSize = datum.maxSize();
        assert datumSize >= 500 && datumSize % 8 == 0;
        long start = System.nanoTime();
        try (ExcerptAppender appender = q0.acquireAppender()) {

            Thread pretoucher = new Thread(() -> {
                ExcerptAppender appender0 = q0.acquireAppender();
                Thread thread = Thread.currentThread();
                while (!thread.isInterrupted()) {
                    appender0.pretouch();
                    Jvm.pause(10);
                }
            });
            pretoucher.setDaemon(true);
            pretoucher.start();

            for (int i = 1; i <= count; i++) {
                long end = start + (long) intervalNS * i;
                while (System.nanoTime() <= end)
                    Thread.yield();

                try (DocumentContext dc = appender.writingDocument()) {
                    Wire wire = dc.wire();
                    wire.write("data");
                    Bytes<?> bytes = wire.bytes();
                    long wp = bytes.writePosition();
                    int paddingToAdd = (int) (-wp & 7);
                    wire.addPadding(paddingToAdd);
                    datum.bytesStore(bytes, bytes.writePosition(), datumSize);
                    bytes.writeSkip(datumSize);

                    // write the data
                    populateDatum(count, datum, i);
                }
            }
            pretoucher.interrupt();
            try {
                pretoucher.join(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long time = System.nanoTime() - start;
        if (!WARMUP) {
            System.out.println("Producer wrote " + count + " messages in " + time / 1000 / 1e6 + " seconds");
        }
    }

    private static void populateDatum(int count, IFacadeAll datum, int i) {
        final int left = count - i;
        if (left % REPORT_INTERVAL == 0 & !WARMUP & left > 0)
            System.out.println("producer " + left + " left");
        datum.setValue3(left); // the number remaining.
        datum.setTimestampAt(0, System.nanoTime());
    }

    public static void firstStage() {
        IFacadeAll datum = Values.newNativeReference(IFacadeAll.class);
        long datumSize = datum.maxSize();
//        int count = 0;
//        long start = System.nanoTime();
        try (ExcerptTailer tailer = q0.createTailer();
             LongValue value = q0.indexForId("stage1")) {

            while (true) {
                final long index;
                try (final DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        Jvm.nanoPause();
                        continue;
                    }

                    applyFlyweight(datum, datumSize, dc);

                    processStage(datum, 1);
                    index = dc.index();
                }
                // write the index processed
                value.setOrderedValue(index);
//                count++;
                final int left = datum.getValue3();
//                if (left % REPORT_INTERVAL == 0 & !WARMUP & left > 0)
//                    System.out.println("stage " + 1 + ", left " + left);
                if (left == 0) {
//                    long time = System.nanoTime() - start;
//                    if (!WARMUP)
//                        System.out.println("First stage read " + count + " messages in " + time / 1000 / 1e6 + " seconds");
                    break;
                }
            }
        }
    }

    private static void applyFlyweight(IFacadeAll datum, long datumSize, DocumentContext dc) {
        Wire wire = dc.wire();
        String event = wire.readEvent(String.class);
        if (!event.equals("data"))
            throw new IllegalStateException("Expected ata not " + event);
        wire.consumePadding();
        Bytes<?> bytes = wire.bytes();
        datum.bytesStore(bytes, bytes.readPosition(), datumSize);
    }

    private static void runStage(int s) {
        IFacadeAll datum = Values.newNativeReference(IFacadeAll.class);
        long datumSize = datum.maxSize();
//        int count = 0;
//        long start = System.nanoTime();
        try (ExcerptTailer tailer = q0.createTailer();
             LongValue stageP = q0.indexForId("stage" + (s - 1));
             LongValue stage = q0.indexForId("stage" + s)) {

            while (true) {
                try (final DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        Jvm.nanoPause();
                        continue;
                    }
                    while (dc.index() > stageP.getVolatileValue())
                        Jvm.nanoPause();

                    applyFlyweight(datum, datumSize, dc);

                    processStage(datum, s);

                    // write the index processed
                    stage.setOrderedValue(dc.index());
                }
//                count++;
                final int left = datum.getValue3();
//                if (left % REPORT_INTERVAL == 0 & !WARMUP & left > 0)
//                    System.out.println("stage " + s + ", left " + left);
                if (left == 0) {
//                    long time = System.nanoTime() - start;
//                    if (!WARMUP)
//                        System.out.println("Stage " + s + " read " + count + " messages in " + time / 1000 / 1e6 + " seconds");
                    break;
                }
            }
        }
    }

    private static void processStage(IFacadeAll datum, int stage) {
        datum.setTimestampAt(stage, System.nanoTime());
    }

    private static void reportLatenciesForStage(int stages, Histogram latencies) {
        IFacadeAll datum = Values.newNativeReference(IFacadeAll.class);
        long datumSize = datum.maxSize();
        try (ExcerptTailer tailer = q0.createTailer()) {

            while (true) {
                try (final DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        break;

                    applyFlyweight(datum, datumSize, dc);

                    long timeNS = datum.getTimestampAt(stages) - datum.getTimestampAt(0);
                    latencies.sampleNanos(timeNS);
                }
            }
        }
        if (!WARMUP) {
            long p99 = Math.round(latencies.percentile(0.99) / 1000);
            System.out.println("End to end 99%ile latency " + p99 / 1e3 + " ms " + latencies.toMicrosFormat());
        }
    }
}
