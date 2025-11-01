/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.DocumentContext;

import java.util.function.Consumer;

/**
 * Plugin interface for handling documents read from the queue in {@code ChronicleReader}.
 * <p>This interface allows for custom handling of the documents, which can be particularly useful when working with non-textual
 * queues, such as those written in binary format. Implementing this plugin provides a way to process the raw {@link DocumentContext}
 * from the queue.
 */
public interface ChronicleReaderPlugin {

    /**
     * Handle the document from the queue that is read in {@code ChronicleReader}.
     * <p>Implement this method to define how a document should be processed when read from the queue.
     * This method provides access to the raw {@link DocumentContext}.
     *
     * @param dc The document context representing the queue entry
     */
    void onReadDocument(DocumentContext dc);

    /**
     * Handle the document and optionally pass it back to the {@code ChronicleReader} as a text representation.
     * <p>This method allows for additional processing of the document and the ability to convert it to a string form using the
     * provided {@link Consumer}. This is useful when inclusion filters or other processing steps need to be applied.
     *
     * @param dc              The document context representing the queue entry
     * @param messageConsumer A consumer used to pass the text representation back to the {@code ChronicleReader}
     */
    default void onReadDocument(DocumentContext dc, Consumer<String> messageConsumer) {
        onReadDocument(dc);
    }
}
