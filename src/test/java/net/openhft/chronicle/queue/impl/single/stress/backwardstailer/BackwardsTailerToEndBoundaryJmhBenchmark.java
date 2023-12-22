package net.openhft.chronicle.queue.impl.single.stress.backwardstailer;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

@Fork(value = 1, warmups = 1)
@State(Scope.Benchmark)
public class BackwardsTailerToEndBoundaryJmhBenchmark {

    private static final Logger log = LoggerFactory.getLogger(BackwardsTailerToEndPerfAcceptanceTest.class);
    private File queuePath;
    private SingleChronicleQueue queue;
    private ExcerptTailer tailer;

    @Setup(Level.Trial)
    public void setup() {
        queuePath = Paths.get(OS.getTarget(), BackwardsTailerToEndBoundaryJmhBenchmark.class.getSimpleName()).toFile();
        IOTools.deleteDirWithFiles(queuePath);
        RollCycle rollCycle = RollCycles.LARGE_DAILY;
        this.queue = SingleChronicleQueueBuilder.builder()
                .path(queuePath)
                .rollCycle(rollCycle)
                .build();
        log.info("Populating queue with data");
        try (ExcerptAppender appender = queue.acquireAppender()) {
            for (int i = 0; i < rollCycle.defaultIndexSpacing() * rollCycle.defaultIndexCount(); i++) {
                appender.writeText(Integer.toString(i));
            }
        }
        log.info("Finished populating queue with data");
        tailer = queue.createTailer().direction(TailerDirection.BACKWARD);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void run(Blackhole blackhole) {
        blackhole.consume(tailer.toEnd());
    }

    @TearDown(Level.Iteration)
    public void runComplete() {
        tailer.toStart();
    }

    @TearDown(Level.Trial)
    public void complete() {
        closeQuietly(tailer, queue);
        IOTools.deleteDirWithFiles(queuePath);
    }

}
