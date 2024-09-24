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

package net.openhft.chronicle.queue.internal.reader.queueentryreaders;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.reader.QueueEntryReader;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.function.Function;

/**
 * {@code AbstractTailerPollingQueueEntryReader} is an abstract base class for implementing
 * queue entry readers that poll entries from a {@link ExcerptTailer}.
 * <p>
 * It provides a template method pattern where the actual reading logic is handled by
 * subclasses via the {@link #doRead(DocumentContext)} method.
 * The polling method is determined by a function provided during construction.
 */
public abstract class AbstractTailerPollingQueueEntryReader implements QueueEntryReader {

    private final ExcerptTailer tailer;  // The ExcerptTailer used to read from the queue
    private final Function<ExcerptTailer, DocumentContext> pollMethod;  // Function for polling queue entries

    /**
     * Constructs an {@code AbstractTailerPollingQueueEntryReader} with the given tailer and polling method.
     *
     * @param tailer     The {@link ExcerptTailer} to read entries from
     * @param pollMethod A function that specifies how to poll the {@link ExcerptTailer} for entries
     */
    protected AbstractTailerPollingQueueEntryReader(ExcerptTailer tailer, Function<ExcerptTailer, DocumentContext> pollMethod) {
        this.tailer = tailer;
        this.pollMethod = pollMethod;
    }

    /**
     * Reads a single entry from the tailer using the specified polling method.
     * <p>
     * If an entry is present, the {@link #doRead(DocumentContext)} method is called to process the entry.
     *
     * @return {@code true} if an entry was read, {@code false} if no entry was available
     */
    @Override
    public final boolean read() {
        try (DocumentContext dc = pollMethod.apply(tailer)) {
            if (!dc.isPresent()) {
                return false;  // No entry to read
            }
            doRead(dc);  // Delegate entry processing to the subclass
            return true;  // Entry was successfully read and processed
        }
    }

    /**
     * Subclasses must implement this method to define how a document context should be processed.
     *
     * @param documentContext The {@link DocumentContext} to process
     */
    protected abstract void doRead(DocumentContext documentContext);
}
