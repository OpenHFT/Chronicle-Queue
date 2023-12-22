package net.openhft.chronicle.queue.impl.single.stress.backwardstailer;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class BackwardsTailerJmhState {

    private static final Logger log = LoggerFactory.getLogger(BackwardsTailerToEndPerfAcceptanceTest.class);

    protected ExcerptTailer tailer;
    private File queuePath;
    private SingleChronicleQueue queue;

    public void setup(int numberOfEntries) {
        queuePath = Paths.get(OS.getTarget(), BackwardsTailerToEndBoundaryJmhBenchmark.class.getSimpleName()).toFile();
        IOTools.deleteDirWithFiles(queuePath);
        RollCycle rollCycle = RollCycles.LARGE_DAILY;
        this.queue = SingleChronicleQueueBuilder.builder()
                .path(queuePath)
                .rollCycle(rollCycle)
                .build();
        log.info("Populating queue with data");
        try (ExcerptAppender appender = queue.acquireAppender()) {
            for (int i = 0; i < numberOfEntries; i++) {
                appender.writeText(Integer.toString(i));
            }
        }
        log.info("Finished populating queue with data");
        tailer = queue.createTailer().direction(TailerDirection.BACKWARD);
        tailer.moveToIndex(0);
    }

    public void runComplete() {
        tailer.toStart();
    }

    public void complete() {
        closeQuietly(tailer, queue);
        IOTools.deleteDirWithFiles(queuePath);
    }

    public ExcerptTailer tailer() {
        return tailer;
    }

    public SingleChronicleQueue queue() {
        return queue;
    }
}
