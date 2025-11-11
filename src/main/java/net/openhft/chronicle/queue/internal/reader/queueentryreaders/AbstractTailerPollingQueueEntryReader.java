/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

    private final ExcerptTailer tailer;
    private final Function<ExcerptTailer, DocumentContext> pollMethod;

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
                return false;
            }
            doRead(dc);
            return true;
        }
    }

    /**
     * Subclasses must implement this method to define how a document context should be processed.
     *
     * @param documentContext The {@link DocumentContext} to process
     */
    protected abstract void doRead(DocumentContext documentContext);
}
