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

@Deprecated(/* to be moved in x.27 */)
public interface HistoryReader {

    HistoryReader withMessageSink(final Consumer<String> messageSink);

    HistoryReader withBasePath(final Path path);

    HistoryReader withProgress(boolean p);

    HistoryReader withTimeUnit(TimeUnit p);

    HistoryReader withHistosByMethod(boolean b);

    HistoryReader withIgnore(long ignore);

    HistoryReader withMeasurementWindow(long measurementWindow);

    HistoryReader withSummaryOutput(int offset);

    /**
     * set the index to start at
     * @param startIndex start index
     * @return this
     */
    HistoryReader withStartIndex(long startIndex);

    ChronicleHistoryReader withHistoSupplier(Supplier<Histogram> histoSupplier);

    void execute();

    /**
     * Read until the end of the queue, accumulating latency histograms.
     * Can be called repeatedly and will start where last finished
     * @return histograms
     */
    Map<String, Histogram> readChronicle();

    void outputData();

    /**
     * Creates and returns a new history reader that will use
     * the queue located at {@link #withBasePath } provided later.
     *
     * @return a new history reader that will use
     * the queue located at {@link #withBasePath } provided later
     */
    static HistoryReader create() {
        return new ChronicleHistoryReader();
    }

    /**
     * Creates and returns a new history reader that will use
     * the provided {@code queueSupplier } to provide the queue.
     *
     * @return a new history reader that will use
     *         the provided {@code queueSupplier } to provide the queue.
     */
    static HistoryReader create(@NotNull final Supplier<? extends ChronicleQueue> queueSupplier) {
        throw new UnsupportedOperationException("TODO");
    }
}
