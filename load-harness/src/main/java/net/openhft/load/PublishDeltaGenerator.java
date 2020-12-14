package net.openhft.load;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.load.config.ConfigParser;
import net.openhft.load.config.StageConfig;
import net.openhft.load.messages.EightyByteMessage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

public final class PublishDeltaGenerator {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <program> [resource-name]");
        }
        Jvm.setExceptionHandlers((c, m, t) -> {
           // System.out.println(m);
        }, (c, m, t) -> {
           // System.out.println(m);
            t.printStackTrace();
        }, (c, m, t) -> System.out.println(m));

        final ConfigParser configParser = new ConfigParser(args[0]);
        final List<StageConfig> allStageConfigs = configParser.getAllStageConfigs();
        final StageConfig lastStageConfig = allStageConfigs.
                get(allStageConfigs.size() - 1);
        try (
                final SingleChronicleQueue pubQueue =
                        ChronicleQueue.singleBuilder(configParser.getPublisherConfig().outputDir()).build();
                final SingleChronicleQueue queue =
                        ChronicleQueue.singleBuilder(lastStageConfig.getOutputPath()).build();
                final Writer resultsWriter = new FileWriter("publish-deltas.txt", false);
                final Writer s0Pub = new FileWriter("s0-deltas.txt", false);
                final Writer s1Pub = new FileWriter("s1-deltas.txt", false);
                final Writer s0s2Pub = new FileWriter("s0-s2-deltas.txt", false);
                final Writer s1s2Pub = new FileWriter("s1-s2-deltas.txt", false);
        ) {
            final MethodReader reader = pubQueue.createTailer().methodReader(new CapturingReceiver(resultsWriter, m -> m.publishNanos));
            while (reader.readOne()) {
                // report
            }
            final MethodReader methodReader = queue.createTailer().methodReader(new DelegatingReceiver(
                    new CapturingReceiver(s0Pub, m -> m.t0),
                    new CapturingReceiver(s1Pub, m -> m.t1),
                    new CapturingReceiver(s0s2Pub, m -> m.t2, 5),
                    new CapturingReceiver(s1s2Pub, m -> m.t2, 6)
            ));
            while (methodReader.readOne()) {
                // report
            }
        }
    }

    private static final class DelegatingReceiver implements MethodDefinition {
        private final MethodDefinition[] delegates;

        DelegatingReceiver(final MethodDefinition... delegates) {
            this.delegates = delegates;
        }

        @Override
        public void onEightyByteMessage(final EightyByteMessage message) {
            for (MethodDefinition delegate : delegates) {
                delegate.onEightyByteMessage(message);
            }
        }
    }

    private static final class CapturingReceiver implements MethodDefinition {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss nnnnnnnnn");
        private final Writer writer;
        private final ToLongFunction<EightyByteMessage> timestampAccessor;
        private final long maskMatch;
        private long lastPublishNanos = -1L;
        private long lastBatchMillis = -1L;
        private long maxDelta = 0;
        private long previousValue = 0L;

        private CapturingReceiver(final Writer writer, final ToLongFunction<EightyByteMessage> timestampAccessor) {
            this(writer, timestampAccessor, 0);
        }

        private CapturingReceiver(final Writer writer,
                                  final ToLongFunction<EightyByteMessage> timestampAccessor, final long maskMatch) {
            this.writer = writer;
            this.timestampAccessor = timestampAccessor;
            this.maskMatch = maskMatch;
        }

        @Override
        public void onEightyByteMessage(final EightyByteMessage message) {
            final long currentVal = timestampAccessor.applyAsLong(message);
            if (currentVal == Long.MAX_VALUE) {
                return;
            }
            if (maskMatch != 0 && message.stagesToPublishBitMask != maskMatch) {
                return;
            }
            if (lastPublishNanos == -1L) {
                lastPublishNanos = currentVal;
                lastBatchMillis = message.batchStartMillis;
                writeToResults("time max_delta_us\n");
            }

            if (lastBatchMillis != message.batchStartMillis) {
                final long deltaMicros = TimeUnit.NANOSECONDS.toMicros(maxDelta);
                final String logMsg = String.format("%s %d%n",
                        FORMATTER.format(LocalDateTime.ofEpochSecond(lastBatchMillis / 1000, ((int) (lastBatchMillis % 1000)) * 1_000_000, ZoneOffset.UTC)),
                        deltaMicros);
                writeToResults(logMsg);
                lastPublishNanos = currentVal;
                lastBatchMillis = message.batchStartMillis;
                maxDelta = 0L;
            }
            maxDelta = Math.max(maxDelta, currentVal - previousValue);
            previousValue = currentVal;
        }

        private void writeToResults(final String logMsg) {
            try {
                writer.append(logMsg);
            } catch (IOException e) {
                System.err.println("Failed to write: " + e.getMessage());
            }
        }
    }
}
