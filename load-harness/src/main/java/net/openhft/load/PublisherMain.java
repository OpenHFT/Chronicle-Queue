package net.openhft.load;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.load.config.ConfigParser;
import net.openhft.load.config.PublisherConfig;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.concurrent.Executors;

public final class PublisherMain {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <program> [resource-name]");
        }
        final ConfigParser configParser = new ConfigParser(args[0]);

        final PublisherConfig publisherConfig = configParser.getPublisherConfig();
        Executors.newSingleThreadExecutor().submit(
                new PretoucherTask(outputQueue(publisherConfig.outputDir()),
                        configParser.getPretouchIntervalMillis()));

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
        return SingleChronicleQueueBuilder.binary(path).sourceId(0).
                rollCycle(RollCycles.HOURLY).build();
    }
}
