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

import net.openhft.chronicle.wire.DocumentContext;

import java.util.function.Consumer;

/**
 * Plugin interface for handling documents read from the queue in {@code ChronicleReader}.
 * <p>This interface allows for custom handling of the documents, which can be particularly useful when working with non-textual
 * queues, such as those written in binary format. Implementing this plugin provides a way to process the raw {@link DocumentContext}
 * from the queue.</p>
 */
public interface ChronicleReaderPlugin {

    /**
     * Handle the document from the queue that is read in {@code ChronicleReader}.
     * <p>Implement this method to define how a document should be processed when read from the queue.
     * This method provides access to the raw {@link DocumentContext}.</p>
     *
     * @param dc The document context representing the queue entry
     */
    void onReadDocument(DocumentContext dc);

    /**
     * Handle the document and optionally pass it back to the {@code ChronicleReader} as a text representation.
     * <p>This method allows for additional processing of the document and the ability to convert it to a string form using the
     * provided {@link Consumer}. This is useful when inclusion filters or other processing steps need to be applied.</p>
     *
     * @param dc              The document context representing the queue entry
     * @param messageConsumer A consumer used to pass the text representation back to the {@code ChronicleReader}
     */
    default void onReadDocument(DocumentContext dc, Consumer<String> messageConsumer) {
        onReadDocument(dc); // Default behavior calls the standard document handling method
    }
}
