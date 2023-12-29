package net.openhft.chronicle.queue.impl.single.stress.backwardstailer;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * The purpose of this main class is to create a large cycle full of data in order to measure the performance of tailer.toEnd()
 * to move to the end of the queue. The queue is about 25-30GB in size depending on the message count provided so please
 * make sure you have sufficient disk space prior to running. The test will take a *long* time to run as it performs a
 * warmup of tailer.toEnd() to remove noise from the final result.
 */
public class LargeCycleFileToEndPerfMain {

    private static final Logger log = LoggerFactory.getLogger(LargeCycleFileToEndPerfMain.class);
    private static final int MESSAGE_COUNT = LargeRollCycles.LARGE_DAILY.defaultIndexCount() * LargeRollCycles.LARGE_DAILY.defaultIndexSpacing() * 25;
    private static final int WARMUP_ITERATIONS = 10_000;
    private static final int MESSAGE_SIZE_BYTES = 256;

    public static void main(String[] args) {
        File path = Paths.get(OS.getTarget(), LargeCycleFileToEndPerfMain.class.getSimpleName()).toFile();
        cleanup(path);
        RollCycle rollCycle = LargeRollCycles.LARGE_DAILY;
        SetTimeProvider timeProvider = new SetTimeProvider();
        timeProvider.set(System.currentTimeMillis());

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder()
                .timeProvider(timeProvider)
                .rollCycle(rollCycle)
                .path(path)
                .build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer().direction(TailerDirection.BACKWARD)) {
            populateQueueData(appender);
            warmupToEnd(tailer);
            measureToEndPerf(tailer, appender);
        } finally {
            cleanup(path);
        }
    }

    private static void cleanup(File path) {
        log.info("Cleaning up {}", path);
        IOTools.deleteDirWithFiles(path);
        log.info("Cleaned up {}", path);
    }

    private static void measureToEndPerf(ExcerptTailer tailer, ExcerptAppender appender) {
        line();
        log.info("Measuring single shot tailer.toEnd() performance");
        log.info("tailer.index()[before]: " + tailer.index());
        long start = System.nanoTime();
        tailer.toEnd();
        long elapsed = System.nanoTime() - start;
        log.info("lastIndexAppended: " + appender.lastIndexAppended());
        log.info("tailer.index()[after]: " + tailer.index());
        log.info("Elapsed time micros: {}us", TimeUnit.NANOSECONDS.toMicros(elapsed));
        log.info("Elapsed time millis: {}ms", TimeUnit.NANOSECONDS.toMillis(elapsed));
        line();
    }

    private static void warmupToEnd(ExcerptTailer tailer) {
        line();
        log.info("Running warm up");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            writePercentComplete(i, WARMUP_ITERATIONS, 500, "tailer.toEnd() warmup");
            tailer.toEnd();
            tailer.toStart();
            System.gc();
        }
        log.info("Warm up complete");
    }

    private static void populateQueueData(ExcerptAppender appender) {
        line();
        VanillaBytes<Void> bytes = Bytes.allocateDirect(MESSAGE_SIZE_BYTES);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            writePercentComplete(i, MESSAGE_COUNT, 1_000_000, "Queue data population");
            bytes.writePosition(MESSAGE_SIZE_BYTES);
            bytes.readPosition(0);
            appender.writeBytes(bytes);
        }
        log.info("Queue generation is 100% complete");
    }

    private static void writePercentComplete(int i, int total, int emitEveryNth, String prefix) {
        if (i % emitEveryNth == 0) {
            log.info("{} is {}% complete", prefix, (int) ((double) i / total * 100));
        }
    }

    private static void line() {
        log.info("--------------------------------------------------------------------------------");
    }

}
