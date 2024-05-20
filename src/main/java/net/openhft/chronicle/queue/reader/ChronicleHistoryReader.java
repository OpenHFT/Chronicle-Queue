/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.util.ToolsUtil;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings("deprecation")
public class ChronicleHistoryReader implements HistoryReader, Closeable {

    private static final int SUMMARY_OUTPUT_UNSET = -999;
    public static final String SEPARATOR = "_";
    protected Path basePath;
    protected Consumer<String> messageSink;
    protected boolean progress = false;
    protected TimeUnit timeUnit = TimeUnit.NANOSECONDS;
    protected boolean histosByMethod = false;
    protected Map<String, Histogram> histos = new LinkedHashMap<>();
    protected long ignore = 0;
    protected long counter = 0;
    protected long measurementWindowNanos = 0;
    protected long firstTimeStampNanos = 0;
    protected long lastWindowCount = 0;
    protected int summaryOutputOffset = SUMMARY_OUTPUT_UNSET;
    protected Long startIndex;
    protected Supplier<Histogram> histoSupplier = () -> new Histogram(60, 4);
    protected int lastHistosSize = 0;
    protected ExcerptTailer tailer;

    static {
        ToolsUtil.warnIfResourceTracing();
    }

    @Override
    public ChronicleHistoryReader withMessageSink(final Consumer<String> messageSink) {
        this.messageSink = messageSink;
        return this;
    }

    @Override
    public ChronicleHistoryReader withBasePath(final Path path) {
        this.basePath = path;
        return this;
    }

    @Override
    public ChronicleHistoryReader withProgress(boolean p) {
        this.progress = p;
        return this;
    }

    @Override
    public ChronicleHistoryReader withTimeUnit(TimeUnit p) {
        this.timeUnit = p;
        return this;
    }

    @Override
    public ChronicleHistoryReader withHistosByMethod(boolean b) {
        this.histosByMethod = b;
        return this;
    }

    @Override
    public ChronicleHistoryReader withIgnore(long ignore) {
        this.ignore = ignore;
        return this;
    }

    @Override
    public ChronicleHistoryReader withMeasurementWindow(long measurementWindow) {
        this.measurementWindowNanos = timeUnit.toNanos(measurementWindow);
        return this;
    }

    @Override
    public ChronicleHistoryReader withSummaryOutput(int offset) {
        this.summaryOutputOffset = offset;
        return this;
    }

    @Override
    public ChronicleHistoryReader withStartIndex(long startIndex) {
        this.startIndex = startIndex;
        return this;
    }

    @Override
    public ChronicleHistoryReader withHistoSupplier(Supplier<Histogram> histoSupplier) {
        this.histoSupplier = histoSupplier;
        return this;
    }

    // TODO: rename as queue is now cached
    @NotNull
    protected ChronicleQueue createQueue() {
        if (tailer != null && ! tailer.queue().isClosed()) {
            return tailer.queue();
        }
        if (!Files.exists(basePath)) {
            throw new IllegalArgumentException(String.format("Path %s does not exist", basePath));
        }
        // TODO: allow builder to be overridden
        SingleChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(basePath.toFile())
                .readOnly(true)
                .build();
        tailer = queue.createTailer();
        if (startIndex != null && !tailer.moveToIndex(startIndex))
            throw new IllegalArgumentException("Could not move to startIndex " + Long.toHexString(startIndex));
        return queue;
    }

    @Override
    public void execute() {
        readChronicle();
        if (measurementWindowNanos == 0)
            outputData();
    }

    @Override
    public Map<String, Histogram> readChronicle() {
        createQueue();
        resetHistos();

        // we have to create MR every time so that it refers to our MessageHistory
        final WireParselet parselet = parselet();
        final FieldNumberParselet fieldNumberParselet = (methodId, wire) -> parselet.accept(methodIdToName(methodId), wire.read());
        final MessageHistory prev = MessageHistory.get();
        MessageHistory.set(new VanillaMessageHistory());
        try (final MethodReader mr = new VanillaMethodReader(tailer, true, parselet, fieldNumberParselet, null, parselet)) {
            while (!Thread.currentThread().isInterrupted() && mr.readOne()) {
                ++counter;
                if (this.progress && counter % 1_000_000L == 0) {
                    Jvm.debug().on(getClass(), "Progress: " + counter);
                }
            }
        } finally {
            MessageHistory.set(prev);
        }

        return histos;
    }

    @NotNull
    protected String methodIdToName(long methodId) {
        return Long.toString(methodId);
    }

    @Override
    public void outputData() {
        if (summaryOutputOffset != SUMMARY_OUTPUT_UNSET)
            printSummary();
        else
            printPercentilesSummary();
    }

    private void printPercentilesSummary() {
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

    private void printSummary() {
        if (histos.size() > lastHistosSize) {
            messageSink.accept("relative_ts," + String.join(",", histos.keySet()));
            lastHistosSize = histos.size();
        }
        long tsSinceStart = (lastWindowCount * measurementWindowNanos) - firstTimeStampNanos;
        messageSink.accept(
                timeUnit.convert(tsSinceStart, TimeUnit.NANOSECONDS) + "," +
                        histos.values().stream().
                                map(h -> Long.toString(timeUnit.convert((long) offset(h.getPercentiles(), summaryOutputOffset), TimeUnit.NANOSECONDS))).
                                collect(Collectors.joining(",")));
    }

    private double offset(double[] percentiles, int offset) {
        return offset >= 0 ? percentiles[offset] : percentiles[percentiles.length + offset];
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
            sb.append(String.format("%12d ", timeUnit.convert((long) value, TimeUnit.NANOSECONDS)));
        });
        return sb.toString();
    }

    protected WireParselet parselet() {
        return (methodName, v) -> {
            v.skipValue();
            if (counter < ignore)
                return;
            final MessageHistory history = MessageHistory.get();
            if (history == null)
                return;

            processMessage(methodName, history);

            if (history.timings() > 0) {
                long firstTiming = history.timing(0);
                if (measurementWindowNanos > 0) {
                    long windowCount = firstTiming / measurementWindowNanos;
                    if (windowCount > lastWindowCount) {
                        windowPassed();
                        lastWindowCount = windowCount;
                    }
                    if (firstTimeStampNanos == 0)
                        firstTimeStampNanos = firstTiming;
                }
            }
        };
    }

    protected void processMessage(CharSequence methodName, MessageHistory history) {
        CharSequence extraHistoId = histosByMethod ? (SEPARATOR + methodName) : "";
        long lastTime = 0;
        // if the tailer has recordHistory (sourceId != 0) then the MessageHistory will be
        // written with a single timing and nothing else. This is then carried through
        int firstWriteOffset = history.timings() - (history.sources() * 2);
        if (!(firstWriteOffset == 0 || firstWriteOffset == 1)) {
            Jvm.warn().on(getClass(), "firstWriteOffset is not 0 or 1 for " + history);
            return;
        }
        for (int sourceIndex = 0; sourceIndex < history.sources(); sourceIndex++) {
            String histoId = Integer.toString(history.sourceId(sourceIndex)) + extraHistoId;
            Histogram histo = histos.computeIfAbsent(histoId, s -> histogram());
            long receivedByThisComponent = history.timing((2 * sourceIndex) + firstWriteOffset);
            long processedByThisComponent = history.timing((2 * sourceIndex) + firstWriteOffset + 1);
            histo.sample((double) (processedByThisComponent - receivedByThisComponent));
            if (lastTime == 0 && firstWriteOffset > 0) {
                Histogram histo1 = histos.computeIfAbsent("startTo" + histoId, s -> histogram());
                histo1.sample((double) (receivedByThisComponent - history.timing(0)));
            } else if (lastTime != 0) {
                Histogram histo1 = histos.computeIfAbsent(history.sourceId(sourceIndex - 1) + "to" + histoId, s -> histogram());
                // here we are comparing System.nanoTime across processes. YMMV
                histo1.sample((double) (receivedByThisComponent - lastTime));
            }
            lastTime = processedByThisComponent;
        }
        if (history.sources() > 1) {
            Histogram histoE2E = histos.computeIfAbsent("endToEnd", s -> histogram());
            histoE2E.sample((double) (history.timing(history.timings() - 1) - history.timing(0)));
        }
    }

    protected void windowPassed() {
        outputData();
        resetHistos();
    }

    private void resetHistos() {
        histos.values().forEach(Histogram::reset);
    }

    @NotNull
    protected Histogram histogram() {
        return histoSupplier.get();
    }

    @Override
    public void close() {
        if (tailer != null)
            tailer.queue().close();
    }
}
