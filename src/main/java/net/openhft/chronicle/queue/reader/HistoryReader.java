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

import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Interface representing a history reader, designed to read from a {@link ChronicleQueue}
 * and collect latency histograms from the queue entries over time.
 * <p>This interface provides methods for configuring the reader, managing message sinks,
 * and accumulating histograms for performance analysis. It supports flexible options such as
 * setting the base path, start index, measurement windows, and other performance metrics.</p>
 */
public interface HistoryReader {

    /**
     * Sets the message sink to handle output messages processed by the reader.
     *
     * @param messageSink The consumer for processing message strings
     * @return The current instance of {@link HistoryReader}
     */
    HistoryReader withMessageSink(final Consumer<String> messageSink);

    /**
     * Sets the base path for the {@link ChronicleQueue} that the history reader will operate on.
     *
     * @param path The base directory path for the Chronicle Queue
     * @return The current instance of {@link HistoryReader}
     */
    HistoryReader withBasePath(final Path path);

    /**
     * Enables or disables progress reporting.
     *
     * @param p {@code true} to enable progress reporting, {@code false} otherwise
     * @return The current instance of {@link HistoryReader}
     */
    HistoryReader withProgress(boolean p);

    /**
     * Sets the time unit for measurements.
     *
     * @param p The {@link TimeUnit} to be used for time-based measurements
     * @return The current instance of {@link HistoryReader}
     */
    HistoryReader withTimeUnit(TimeUnit p);

    /**
     * Enables or disables histograms by method.
     *
     * @param b {@code true} to enable histograms by method, {@code false} otherwise
     * @return The current instance of {@link HistoryReader}
     */
    HistoryReader withHistosByMethod(boolean b);

    /**
     * Sets the number of initial messages to ignore at the start.
     *
     * @param ignore The number of messages to ignore
     * @return The current instance of {@link HistoryReader}
     */
    HistoryReader withIgnore(long ignore);

    /**
     * Sets the measurement window size in the configured time unit.
     *
     * @param measurementWindow The size of the measurement window
     * @return The current instance of {@link HistoryReader}
     */
    HistoryReader withMeasurementWindow(long measurementWindow);

    /**
     * Configures the offset for summary output.
     *
     * @param offset The offset for summary output
     * @return The current instance of {@link HistoryReader}
     */
    HistoryReader withSummaryOutput(int offset);

    /**
     * Sets the start index for reading the queue.
     *
     * @param startIndex The start index to begin reading from
     * @return The current instance of {@link HistoryReader}
     */
    HistoryReader withStartIndex(long startIndex);

    /**
     * Sets the supplier for histograms.
     *
     * @param histoSupplier The supplier for providing histograms
     * @return The current instance of {@link ChronicleHistoryReader}
     */
    ChronicleHistoryReader withHistoSupplier(Supplier<Histogram> histoSupplier);

    /**
     * Executes the history reader to process messages from the queue.
     */
    void execute();

    /**
     * Reads messages from the queue until the end, accumulating latency histograms.
     * This method can be called repeatedly and will continue from where the last call finished.
     *
     * @return A map of histograms representing message processing metrics
     */
    Map<String, Histogram> readChronicle();

    /**
     * Outputs the collected data from the histograms.
     */
    void outputData();

    /**
     * Creates and returns a new history reader that will use the queue located at the path
     * provided later via {@link #withBasePath}.
     *
     * @return A new instance of {@link HistoryReader}
     */
    static HistoryReader create() {
        return new ChronicleHistoryReader();
    }

    /**
     * Creates and returns a new history reader that will use the provided queue supplier
     * to obtain the {@link ChronicleQueue}.
     *
     * @param queueSupplier A supplier providing the {@link ChronicleQueue} to be used
     * @return A new instance of {@link HistoryReader}
     */
    static HistoryReader create(@NotNull final Supplier<? extends ChronicleQueue> queueSupplier) {
        throw new UnsupportedOperationException("TODO"); // Implementation pending
    }
}
