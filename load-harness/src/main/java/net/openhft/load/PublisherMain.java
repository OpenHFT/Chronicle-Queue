package net.openhft.load;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.load.config.ConfigParser;
import net.openhft.load.config.PublisherConfig;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class PublisherMain {
    public static void main(String[] args) throws Exception {
       // MlockAll.doMlockall();
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <program> [resource-name]");
        }
        final ConfigParser configParser = new ConfigParser(args[0]);

        final PublisherConfig publisherConfig = configParser.getPublisherConfig();
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(
                new PretoucherTask(outputQueue(publisherConfig.outputDir()),
                        configParser.getPretouchIntervalMillis()));

        executorService.submit(new HiccupReporter()::start);

        final Publisher publisher = new Publisher(publisherConfig, createOutput(publisherConfig.outputDir()),
                configParser.getAllStageConfigs());
        publisher.init();
        publisher.startPublishing();
    }

    private static MethodDefinition createOutput(final Path path) {
        final SingleChronicleQueue queue = outputQueue(path);
        final ExcerptAppender appender = queue.acquireAppender();

        return new GarbageFreeMethodPublisher(() -> appender);
    }

    @NotNull
    private static SingleChronicleQueue outputQueue(final Path path) {
        return ChronicleQueue.singleBuilder(path).sourceId(0).
                rollTime(RollTimeCalculator.getNextRollWindow(), ZoneId.of("UTC")).
                rollCycle(RollCycles.HOURLY).build();
    }

    private static class HiccupReporter {

        void start() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final long start = System.nanoTime();
                    Thread.sleep(1);
                    final long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                    if (durationMillis > 50L) {
                       // System.out.println("Schedule jitter: " + durationMillis + "ms at " + Instant.now());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
