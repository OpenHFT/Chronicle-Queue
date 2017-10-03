package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.MethodReader;
import net.openhft.chronicle.wire.VanillaMessageHistory;
import net.openhft.chronicle.wire.WireParselet;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by Jerry Shea on 29/09/17.
 */
public class ChronicleHistoryReader {

    private Path basePath;
    private Consumer<String> messageSink;
    private Map<String, Histogram> histos = new LinkedHashMap<>();
    private boolean progress = false;

    public ChronicleHistoryReader withMessageSink(final Consumer<String> messageSink) {
        this.messageSink = messageSink;
        return this;
    }

    public ChronicleHistoryReader withBasePath(final Path path) {
        this.basePath = path;
        return this;
    }

    public ChronicleHistoryReader withProgress(boolean p) {
        this.progress = p;
        return this;
    }

    @NotNull
    private SingleChronicleQueue createQueue() {
        if (!Files.exists(basePath)) {
            throw new IllegalArgumentException(String.format("Path %s does not exist", basePath));
        }
        return SingleChronicleQueueBuilder
                .binary(basePath.toFile())
                .readOnly(true)
                .build();
    }

    public void execute() {
        final SingleChronicleQueue q = createQueue();
        final ExcerptTailer tailer = q.createTailer();
        final WireParselet parselet = parselet();
        final MethodReader mr = new MethodReader(tailer, true, parselet, null, parselet);

        MessageHistory.set(new VanillaMessageHistory());
        int counter = 0;
        while (! Thread.currentThread().isInterrupted() && mr.readOne()) {
            ++counter;
            if (this.progress && counter % 1_000_000 == 0) {
                System.out.println("Progress: " + counter);
            }
        }

        printPercentilesSummary();
    }

    private void printPercentilesSummary() {
        // we should also consider the case where >1 output messages are from 1 incoming

        int counter = 0;
        messageSink.accept("Timings below in ns");
        final StringBuilder sb = new StringBuilder("sourceId        ");
        histos.forEach((id, histogram) -> sb.append(String.format("%12s ", id)));
        messageSink.accept(sb.toString());
        messageSink.accept("50:     " + percentiles(counter++));
        messageSink.accept("90:     " + percentiles(counter++));
        messageSink.accept("99:     " + percentiles(counter++));
        messageSink.accept("99.9:   " + percentiles(counter++));
        messageSink.accept("99.99:  " + percentiles(counter++));
        messageSink.accept("99.999: " + percentiles(counter++));
        messageSink.accept("99.9999:" + percentiles(counter++));
        messageSink.accept("worst:  " + percentiles(-1));
    }

    private String percentiles(final int index) {
        final StringBuilder sb = new StringBuilder("        ");
        histos.forEach((id, histogram) -> {
            if (index >= histogram.getPercentiles().length - 1) {
                sb.append(String.format("%12s ", " "));
                return;
            }
            int myIndex = index;
            if (myIndex == -1) myIndex = histogram.getPercentiles().length - 1;
            double value = histogram.getPercentiles()[myIndex];
            sb.append(String.format("%12.0f ", value));
        });
        return sb.toString();
    }

    private WireParselet parselet() {
        return (methodName, v, $) -> {
            v.skipValue();
            final MessageHistory history = MessageHistory.get();
            long lastTime = 0;
            for (int sourceIndex=0; sourceIndex<history.sources(); sourceIndex++) {
                String sourceId = Integer.toString(history.sourceId(sourceIndex));
                Histogram histo = histos.computeIfAbsent(sourceId, s -> new Histogram());
                long receivedByThisComponent = history.timing(2 * sourceIndex);
                long processedByThisComponent = history.timing((2 * sourceIndex) + 1);
                histo.sample(processedByThisComponent - receivedByThisComponent);
                if (lastTime != 0) {
                    Histogram histo1 = histos.computeIfAbsent(Integer.toString(history.sourceId(sourceIndex-1)) + "to" + sourceId, s -> new Histogram());
                    // here we are comparing System.nanoTime across processes. YMMV
                    histo1.sample(receivedByThisComponent - lastTime);
                }
                lastTime = processedByThisComponent;
            }
        };
    }
}
