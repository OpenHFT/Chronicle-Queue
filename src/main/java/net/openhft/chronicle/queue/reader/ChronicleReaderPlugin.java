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
 * Handle the document from the queue that is read in {@code ChronicleReader}.
 * Particularly useful when you need more than the text representation e.g.
 * when your queue is written in binary.
 */
public interface ChronicleReaderPlugin {
    void onReadDocument(DocumentContext dc);

    /**
     * Consume dc and allow it to be given back to ChronicleReader so it could e.g. apply inclusion filters
     *
     * @param dc              doc context
     * @param messageConsumer use this to pass back text representation
     */
    default void onReadDocument(DocumentContext dc, Consumer<String> messageConsumer) {
        onReadDocument(dc);
    }
}
