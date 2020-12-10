package net.openhft.load;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.load.config.ConfigParser;
import net.openhft.load.config.StageConfig;
import net.openhft.load.messages.EightyByteMessage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class ResultGenerator {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <program> [resource-name]");
        }

        final ConfigParser configParser = new ConfigParser(args[0]);

        final List<StageConfig> allStageConfigs = configParser.getAllStageConfigs();
        final StageConfig lastStageConfig = allStageConfigs.
                get(allStageConfigs.size() - 1);
        Jvm.setExceptionHandlers((c, m, t) -> {
           // System.out.println(m);
        }, (c, m, t) -> {
           // System.out.println(m);
            if (t != null) {
                t.printStackTrace();
            }
        }, (c, m, t) -> System.out.println(m));
        try (final SingleChronicleQueue queue =
                     ChronicleQueue.singleBuilder(lastStageConfig.getOutputPath()).build();
             final Writer resultsWriter = new FileWriter("results.txt", false)) {
            final MethodReader methodReader = queue.createTailer().methodReader(new CapturingReceiver(resultsWriter));
            while (methodReader.readOne()) {
                // report
            }
        }
    }

    private static final class CapturingReceiver implements MethodDefinition {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
        private static final DateTimeFormatter NANOS_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.nnnnnnnnn");
        private final Writer writer;

        private long currentSecond = -1L;
        private long messagesInThisSecond = 0L;
        private long maxTotalLatencyInThisSecond = 0L;
        private long minTotalLatencyInThisSecond = Long.MAX_VALUE;
        private long totalLatencyInThisSecond = 0L;
        private Histogram latencyHistogram = new Histogram();
        private String worstMessage = null;
        private long[] maxQueueDeltas = new long[0];
        private EightyByteMessage worstCopy = new EightyByteMessage();

        private CapturingReceiver(final Writer writer) {
            this.writer = writer;
        }

        private static long getLatestTimestamp(final EightyByteMessage message) {
            if (EightyByteMessage.isSet(message.t6)) {
                return message.t6;
            }
            if (EightyByteMessage.isSet(message.t5)) {
                return message.t5;
            }
            if (EightyByteMessage.isSet(message.t4)) {
                return message.t4;
            }
            if (EightyByteMessage.isSet(message.t3)) {
                return message.t3;
            }
            if (EightyByteMessage.isSet(message.t2)) {
                return message.t2;
            }
            if (EightyByteMessage.isSet(message.t1)) {
                return message.t1;
            }
            if (EightyByteMessage.isSet(message.t0)) {
                return message.t0;
            }
            throw new IllegalStateException("No values were set on message: " + Marshallable.$toString(message));
        }

        @Override
        public void onEightyByteMessage(final EightyByteMessage message) {
            final MessageHistory messageHistory = MessageHistory.get();
            if (maxQueueDeltas.length == 0) {
                maxQueueDeltas = new long[(messageHistory.timings() - 1)];
            }
            long batchStartEpochSeconds = message.batchStartMillis / 1000;

            for (int i = 0; i < messageHistory.timings() - 1; i++) {
                maxQueueDeltas[i] = Math.max(messageHistory.timing(i + 1) - messageHistory.timing(i),
                        maxQueueDeltas[i]);
            }

            if (currentSecond == -1L) {
                currentSecond = batchStartEpochSeconds;
                writeToResults("time msg/sec min max avg 50% 99% 99.99% batch_time");
                for (int i = 0; i < maxQueueDeltas.length; i++) {
                    writeToResults(" q_" + i);
                }
                writeToResults("\n");
            }
            if (batchStartEpochSeconds < currentSecond) {
                batchStartEpochSeconds = currentSecond;
            }

            if (batchStartEpochSeconds != currentSecond) {
                final long batchTimeSeconds = batchStartEpochSeconds - currentSecond;
                final String logMsg = String.format("%s %d %d %d %d %d %d %d %d",
                        FORMATTER.format(LocalDateTime.ofEpochSecond(batchStartEpochSeconds, 0, ZoneOffset.UTC)),
                        messagesInThisSecond,
                        TimeUnit.NANOSECONDS.toMicros(minTotalLatencyInThisSecond),
                        TimeUnit.NANOSECONDS.toMicros(maxTotalLatencyInThisSecond),
                        TimeUnit.NANOSECONDS.toMicros(totalLatencyInThisSecond / messagesInThisSecond),
                        TimeUnit.NANOSECONDS.toMicros((long) latencyHistogram.percentile(0.50d)),
                        TimeUnit.NANOSECONDS.toMicros((long) latencyHistogram.percentile(0.99d)),
                        TimeUnit.NANOSECONDS.toMicros((long) latencyHistogram.percentile(0.9999d)),
                        batchTimeSeconds);
                writeToResults(logMsg);
                for (int i = 0; i < maxQueueDeltas.length; i++) {
                    writeToResults(" " + TimeUnit.NANOSECONDS.toMicros(maxQueueDeltas[i]));
                }
                writeToResults("\n");
               // System.out.println("Worst in second");
               // System.out.println(FORMATTER.format(LocalDateTime.ofEpochSecond(worstCopy.batchStartMillis / 1000, 0, ZoneOffset.UTC)));
               // System.out.println("publish to first stage");
                if (worstCopy.stagesToPublishBitMask == 5) {
                   // System.out.println(TimeUnit.NANOSECONDS.toMicros(worstCopy.t0 - worstCopy.publishNanos));
                } else {
                   // System.out.println(TimeUnit.NANOSECONDS.toMicros(worstCopy.t1 - worstCopy.publishNanos));
                }
               // System.out.println("first to second stage");
                if (worstCopy.stagesToPublishBitMask == 5) {
                   // System.out.println(TimeUnit.NANOSECONDS.toMicros(worstCopy.t2 - worstCopy.t0));
                } else {
                   // System.out.println(TimeUnit.NANOSECONDS.toMicros(worstCopy.t2 - worstCopy.t1));
                }
               // System.out.println(worstMessage);

                messagesInThisSecond = 0L;
                maxTotalLatencyInThisSecond = 0L;
                minTotalLatencyInThisSecond = Long.MAX_VALUE;
                totalLatencyInThisSecond = 0L;
                currentSecond = batchStartEpochSeconds;
                latencyHistogram.reset();
                worstMessage = null;
                Arrays.fill(maxQueueDeltas, 0L);
            }
            messagesInThisSecond++;
            final long totalMessageLatency = getLatestTimestamp(message) - message.publishNanos;
            maxTotalLatencyInThisSecond = Math.max(maxTotalLatencyInThisSecond,
                    totalMessageLatency);
            if (maxTotalLatencyInThisSecond == totalMessageLatency) {
                worstMessage = Marshallable.$toString(message);
                message.copyTo(worstCopy);
            }
            minTotalLatencyInThisSecond = Math.min(minTotalLatencyInThisSecond, totalMessageLatency);
            totalLatencyInThisSecond += totalMessageLatency;
            latencyHistogram.sampleNanos(totalMessageLatency);
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
