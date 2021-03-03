package run.chronicle.staged;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PerformanceMain {
    static final String PATH = System.getProperty("path", OS.TMP);
    static final int THROUGHPUT = Integer.getInteger("throughput", 500_000);
    public static final int REPORT_INTERVAL = 5_000_000;
    static final int INTERVAL = 1_000_000_000 / THROUGHPUT;
    static String DIR;
    static final int COUNT = Integer.getInteger("count", 30 * THROUGHPUT);
    static final int STAGES;
    static boolean WARMUP;

    static {
        int stages = Integer.getInteger("stages", 10);
        int maxStages = Runtime.getRuntime().availableProcessors() / 2;
        if (stages > maxStages) {
            System.err.println("Not enough CPUs for " + stages + " testing " + maxStages + " stages");
            stages = maxStages;
        }
        STAGES = stages;
        System.out.println("Testing " + STAGES + " stages");
    }

    public static void main(String... args) {
        Jvm.setExceptionHandlers(Jvm.error(), Jvm.warn(), null, null);
        MappedFile.warmup();
        runBenchmark(200_000, 5_000, true);
        System.out.println("Warmup complete");
        runBenchmark(COUNT, INTERVAL, false);
        runBenchmark(COUNT, INTERVAL, false);
        runBenchmark(COUNT, INTERVAL, false);
    }

    private static void runBenchmark(int count, int interval, boolean warmup) {
        String run = Long.toString(System.nanoTime(), 36);
        WARMUP = warmup;
        DIR = PATH + "/run-" + run;

        new File(DIR).mkdirs();

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

        IOTools.deleteDirWithFiles(DIR, 3);
    }

    public static void producer(int count, int intervalNS) {
        IFacadeAll datum = Values.newNativeReference(IFacadeAll.class);
        long datumSize = datum.maxSize();
        assert datumSize >= 500;
        long start = System.nanoTime();
        try (ChronicleQueue q0 = SingleChronicleQueueBuilder.binary(DIR + "/data").build();
             ExcerptAppender appender = q0.acquireAppender()) {

            for (int i = 1; i <= count; i++) {
                long end = start + (long) intervalNS * i;
                while (System.nanoTime() <= end)
                    Jvm.nanoPause();

                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().write("data");
                    Bytes<?> bytes = dc.wire().bytes();
                    datum.bytesStore(bytes, bytes.writePosition(), datumSize);
                    bytes.writeSkip(datumSize);

                    // write the data
                    populateDatum(count, datum, i);
                }
            }
        }
        long time = System.nanoTime() - start;
        if (!WARMUP)
            System.out.println("Producer wrote " + count + " messages in " + time / 1000 / 1e6 + " seconds");
    }

    private static void populateDatum(int count, IFacadeAll datum, int i) {
        final int left = count - i;
        if (left % REPORT_INTERVAL == 0 & !WARMUP)
            System.out.println("producer " + left + " left");
        datum.setValue3(left); // the number remaining.
        datum.setTimestampAt(0, System.nanoTime());
    }

    public static void firstStage() {
        IFacadeAll datum = Values.newNativeReference(IFacadeAll.class);
        long datumSize = datum.maxSize();
        int count = 0;
        long start = System.nanoTime();
        try (ChronicleQueue q0 = SingleChronicleQueueBuilder.binary(DIR + "/data").build();
             ExcerptTailer tailer = q0.createTailer();
             ChronicleQueue q1 = SingleChronicleQueueBuilder.binary(DIR + "/q1").build();
             ExcerptAppender appender = q1.acquireAppender()) {

            while (true) {
                final long index;
                try (final DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        Thread.yield();
                        continue;
                    }

                    String event = dc.wire().readEvent(String.class);
                    if (!event.equals("data"))
                        throw new IllegalStateException("Expected ata not " + event);
                    Bytes<?> bytes = dc.wire().bytes();
                    datum.bytesStore(bytes, bytes.readPosition(), datumSize);

                    processStage(datum, 1);
                    index = dc.index();
                }
                // write the index processed
                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().bytes().writeLong(index);
                }
                count++;
                final int left = datum.getValue3();
                if (left % REPORT_INTERVAL == 0 & !WARMUP)
                    System.out.println("stage " + 1 + ", left " + left);
                if (left == 0) {
                    long time = System.nanoTime() - start;
                    if (!WARMUP)
                        System.out.println("First stage read " + count + " messages in " + time / 1000 / 1e6 + " seconds");
                    break;
                }
            }
        }
    }

    private static void runStage(int s) {
        IFacadeAll datum = Values.newNativeReference(IFacadeAll.class);
        long datumSize = datum.maxSize();
        int count = 0;
        long start = System.nanoTime();
        try (ChronicleQueue q0 = SingleChronicleQueueBuilder.binary(DIR + "/data").build();
             ExcerptTailer tailer = q0.createTailer();
             ChronicleQueue qP = SingleChronicleQueueBuilder.binary(DIR + "/q" + (s - 1)).build();
             ExcerptTailer tailerP = qP.createTailer();
             ChronicleQueue qS = SingleChronicleQueueBuilder.binary(DIR + "/q" + s).build();
             ExcerptAppender appender = qS.acquireAppender()) {

            while (true) {
                final long index;
                try (final DocumentContext dcP = tailerP.readingDocument()) {
                    if (!dcP.isPresent()) {
                        Thread.yield();
                        continue;
                    }

                    index = dcP.wire().bytes().readLong();
                    boolean found = tailer.moveToIndex(index);
                    if (!found)
                        throw new IllegalStateException("Failed to find index " + index);
                    try (DocumentContext dc = tailer.readingDocument()) {
                        String event = dc.wire().readEvent(String.class);
                        if (!event.equals("data"))
                            throw new IllegalStateException("Expected ata not " + event);
                        Bytes<?> bytes = dc.wire().bytes();
                        datum.bytesStore(bytes, bytes.readPosition(), datumSize);

                        processStage(datum, s);
                    }
                }
                // write the index processed
                try (final DocumentContext dc = appender.writingDocument()) {
                    dc.wire().bytes().writeLong(index);
                }
                count++;
                final int left = datum.getValue3();
                if (left % REPORT_INTERVAL == 0 & !WARMUP)
                    System.out.println("stage " + s + ", left " + left);
                if (left == 0) {
                    long time = System.nanoTime() - start;
                    if (!WARMUP)
                        System.out.println("First stage read " + count + " messages in " + time / 1000 / 1e6 + " seconds");
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
        try (ChronicleQueue q0 = SingleChronicleQueueBuilder.binary(DIR + "/data").build();
             ExcerptTailer tailer = q0.createTailer()) {

            while (true) {
                try (final DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        break;

                    String event = dc.wire().readEvent(String.class);
                    if (!event.equals("data"))
                        throw new IllegalStateException("Expected ata not " + event);
                    Bytes<?> bytes = dc.wire().bytes();
                    datum.bytesStore(bytes, bytes.readPosition(), datumSize);

                    long timeNS = datum.getTimestampAt(stages) - datum.getTimestampAt(0);
                    latencies.sampleNanos(timeNS);
                }
            }
        }
        if (!WARMUP)
            System.out.println("End to end latencies " + latencies.toLongMicrosFormat());
    }
}
