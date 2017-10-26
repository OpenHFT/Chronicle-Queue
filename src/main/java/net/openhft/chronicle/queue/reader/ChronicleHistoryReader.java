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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by Jerry Shea on 29/09/17.
 */
public class ChronicleHistoryReader {

    private Path basePath;
    private Consumer<String> messageSink;
    private boolean progress = false;
    private TimeUnit timeUnit = TimeUnit.NANOSECONDS;
    protected boolean histosByMethod = false;
    protected Map<String, Histogram> histos = new LinkedHashMap<>();

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

    public ChronicleHistoryReader withTimeUnit(TimeUnit p) {
        this.timeUnit = p;
        return this;
    }

    public ChronicleHistoryReader withHistosByMethod(boolean b) {
        this.histosByMethod = b;
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
        readChronicle();
        printPercentilesSummary();
    }

    public Map<String, Histogram> readChronicle() {
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

        return histos;
    }

    public void printPercentilesSummary() {
        // we should also consider the case where >1 output messages are from 1 incoming

        if (histos.size() == 0) {
            messageSink.accept("No data");
            return;
        }
        int counter = 0;
        messageSink.accept("Timings below in " + timeUnit.name());
        final StringBuilder sb = new StringBuilder("sourceId        ");
        histos.forEach((id, histogram) -> sb.append(String.format("%12s ", id)));
        messageSink.accept(sb.toString());
        messageSink.accept("count:  " + count());
        messageSink.accept("50:     " + percentiles(counter++));
        messageSink.accept("90:     " + percentiles(counter++));
        messageSink.accept("99:     " + percentiles(counter++));
        messageSink.accept("99.9:   " + percentiles(counter++));
        messageSink.accept("99.99:  " + percentiles(counter++));
        messageSink.accept("99.999: " + percentiles(counter++));
        messageSink.accept("99.9999:" + percentiles(counter++));
        messageSink.accept("worst:  " + percentiles(-1));
    }

    private String count() {
        final StringBuilder sb = new StringBuilder("        ");
        histos.forEach((id, histogram) -> sb.append(String.format("%12d ", histogram.totalCount())));
        return sb.toString();
    }

    private String percentiles(final int index) {
        final StringBuilder sb = new StringBuilder("        ");
        histos.forEach((id, histogram) -> {
            double[] percentiles = histogram.getPercentiles();
            if (index >= percentiles.length - 1) {
                sb.append(String.format("%12s ", " "));
                return;
            }
            int myIndex = index;
            if (myIndex == -1) myIndex = percentiles.length - 1;
            double value = percentiles[myIndex];
            sb.append(String.format("%12d ", timeUnit.convert((long)value, TimeUnit.NANOSECONDS)));
        });
        return sb.toString();
    }

    protected WireParselet parselet() {
        return (methodName, v, $) -> {
            v.skipValue();
            CharSequence extraHistoId = histosByMethod ? ("_"+methodName) : "";
            final MessageHistory history = MessageHistory.get();
            long lastTime = 0;
            // if the tailer has recordHistory(true) then the MessageHistory will be
            // written with a single timing and nothing else. This is then carried through
            int firstWriteOffset = history.timings() - (history.sources() * 2);
            assert firstWriteOffset == 0 || firstWriteOffset == 1;
            for (int sourceIndex=0; sourceIndex<history.sources(); sourceIndex++) {
                String histoId = Integer.toString(history.sourceId(sourceIndex)) + extraHistoId;
                Histogram histo = histos.computeIfAbsent(histoId, s -> histogram());
                long receivedByThisComponent = history.timing((2 * sourceIndex) + firstWriteOffset);
                long processedByThisComponent = history.timing((2 * sourceIndex) + firstWriteOffset + 1);
                histo.sample(processedByThisComponent - receivedByThisComponent);
                if (lastTime == 0 && firstWriteOffset > 0) {
                    Histogram histo1 = histos.computeIfAbsent("startTo" + histoId, s -> histogram());
                    histo1.sample(receivedByThisComponent - history.timing(0));
                } else if (lastTime != 0) {
                    Histogram histo1 = histos.computeIfAbsent(Integer.toString(history.sourceId(sourceIndex-1)) + "to" + histoId, s -> histogram());
                    // here we are comparing System.nanoTime across processes. YMMV
                    histo1.sample(receivedByThisComponent - lastTime);
                }
                lastTime = processedByThisComponent;
            }
            if (history.sources() > 1) {
                Histogram histoE2E = histos.computeIfAbsent("endToEnd", s -> histogram());
                histoE2E.sample(history.timing(history.timings() - 1) - history.timing(0));
            }
        };
    }

    @NotNull
    protected Histogram histogram() {
        return new Histogram(60, 4);
    }
}
