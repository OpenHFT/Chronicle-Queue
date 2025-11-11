/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

/**
 * Implementation of the {@link HistoryReader} interface, providing functionality for reading and processing
 * historical Chronicle Queue data with timing and histogram-based metrics.
 * <p>
 * This class allows the user to read messages from a Chronicle Queue and process them with the help of
 * histograms and timing windows. Various options such as progress reporting, time unit settings, and
 * histogram management are available for customization.
 */
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

    /**
     * Sets the message sink, which is a consumer to handle each processed message.
     *
     * @param messageSink The consumer to handle processed messages
     * @return The current instance of {@link ChronicleHistoryReader}
     */
    @Override
    public ChronicleHistoryReader withMessageSink(final Consumer<String> messageSink) {
        this.messageSink = messageSink;
        return this;
    }

    /**
     * Sets the base path for the Chronicle Queue.
     *
     * @param path The path to the Chronicle Queue directory
     * @return The current instance of {@link ChronicleHistoryReader}
     */
    @Override
    public ChronicleHistoryReader withBasePath(final Path path) {
        this.basePath = path;
        return this;
    }

    /**
     * Enables or disables progress reporting.
     *
     * @param p True to enable progress reporting, false otherwise
     * @return The current instance of {@link ChronicleHistoryReader}
     */
    @Override
    public ChronicleHistoryReader withProgress(boolean p) {
        this.progress = p;
        return this;
    }

    /**
     * Sets the time unit for measurements.
     *
     * @param p The {@link TimeUnit} to be used for time-based measurements
     * @return The current instance of {@link ChronicleHistoryReader}
     */
    @Override
    public ChronicleHistoryReader withTimeUnit(TimeUnit p) {
        this.timeUnit = p;
        return this;
    }

    /**
     * Enables or disables method-specific histograms.
     *
     * @param b True to enable histograms by method, false otherwise
     * @return The current instance of {@link ChronicleHistoryReader}
     */
    @Override
    public ChronicleHistoryReader withHistosByMethod(boolean b) {
        this.histosByMethod = b;
        return this;
    }

    /**
     * Sets the number of messages to ignore at the start.
     *
     * @param ignore The number of messages to ignore
     * @return The current instance of {@link ChronicleHistoryReader}
     */
    @Override
    public ChronicleHistoryReader withIgnore(long ignore) {
        this.ignore = ignore;
        return this;
    }

    /**
     * Sets the measurement window size in the configured time unit.
     *
     * @param measurementWindow The size of the measurement window
     * @return The current instance of {@link ChronicleHistoryReader}
     */
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

    /**
     * Creates and returns a new {@link ChronicleQueue} instance if the current tailer is null or closed.
     * Otherwise, returns the current tailer's queue. This method throws an exception if the base path does not exist.
     *
     * @return A new or cached {@link ChronicleQueue} instance
     * @throws IllegalArgumentException if the base path does not exist or if the start index could not be moved to
     */
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

    /**
     * Executes the reading of the Chronicle Queue and outputs data if no measurement window is set.
     */
    @Override
    public void execute() {
        readChronicle();
        if (measurementWindowNanos == 0)
            outputData();
    }

    /**
     * Reads messages from the Chronicle Queue, processing each one and updating histograms.
     * Progress is reported if enabled, and histograms are returned after reading.
     *
     * @return A map of histograms representing message processing metrics
     */
    @Override
    public Map<String, Histogram> readChronicle() {
        createQueue();
        resetHistos();

        // we have to create MR every time so that it refers to our MessageHistory
        final WireParselet parselet = parselet();
        final FieldNumberParselet fieldNumberParselet = (methodId, wire) -> parselet.accept(methodIdToName(methodId), wire.read());
        final MessageHistory prev = MessageHistory.get();
        MessageHistory.emptyHistory();
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

    /**
     * Converts a method ID to its corresponding name.
     *
     * @param methodId The method ID
     * @return The method name as a string
     */
    @NotNull
    protected String methodIdToName(long methodId) {
        return Long.toString(methodId);
    }

    /**
     * Outputs the data, either as a summary or as percentile-based timings depending on the configuration.
     */
    @Override
    public void outputData() {
        if (summaryOutputOffset != SUMMARY_OUTPUT_UNSET)
            printSummary();
        else
            printPercentilesSummary();
    }

    /**
     * Prints a percentile-based summary of the histograms.
     */
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

    /**
     * Prints a summary of the histograms.
     */
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

    /**
     * Calculates the value at a specified percentile offset.
     *
     * @param percentiles The array of percentile values
     * @param offset      The percentile offset
     * @return The value at the specified percentile
     */
    private double offset(double[] percentiles, int offset) {
        return offset >= 0 ? percentiles[offset] : percentiles[percentiles.length + offset];
    }

    /**
     * Returns a formatted string representing the count of messages processed by each histogram.
     *
     * @return A string representing the message counts
     */
    private String count() {
        final StringBuilder sb = new StringBuilder("        ");
        histos.forEach((id, histogram) -> sb.append(String.format("%12d ", histogram.totalCount())));
        return sb.toString();
    }

    /**
     * Returns a formatted string representing the percentile values for the histograms.
     *
     * @param index The index of the percentile to retrieve
     * @return A string representing the percentile values
     */
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

    /**
     * Creates a {@link WireParselet} for processing Chronicle Queue messages.
     *
     * @return A new {@link WireParselet} instance
     */
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

    /**
     * Processes a message and updates the histograms based on timing data.
     *
     * @param methodName The name of the method being processed
     * @param history    The {@link MessageHistory} for the message
     */
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

    /**
     * Handles when a measurement window has passed, outputting the data and resetting histograms.
     */
    protected void windowPassed() {
        outputData();
        resetHistos();
    }

    /**
     * Resets all histograms to their initial state.
     */
    private void resetHistos() {
        histos.values().forEach(Histogram::reset);
    }

    /**
     * Creates a new {@link Histogram} instance using the configured supplier.
     *
     * @return A new {@link Histogram} instance
     */
    @NotNull
    protected Histogram histogram() {
        return histoSupplier.get();
    }

    /**
     * Closes the tailer and the associated queue.
     */
    @Override
    public void close() {
        if (tailer != null)
            tailer.queue().close();
    }
}
