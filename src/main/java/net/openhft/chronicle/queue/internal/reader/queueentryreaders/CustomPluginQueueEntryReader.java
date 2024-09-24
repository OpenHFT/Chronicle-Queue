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
import net.openhft.chronicle.queue.reader.ChronicleReaderPlugin;
import net.openhft.chronicle.queue.reader.MessageConsumer;
import net.openhft.chronicle.wire.DocumentContext;

import java.util.function.Function;

/**
 * {@code CustomPluginQueueEntryReader} is a specialized queue entry reader that integrates
 * with a custom {@link ChronicleReaderPlugin} for processing queue entries.
 * <p>
 * It uses a {@link MessageConsumer} to handle the processed messages and delegates
 * entry reading to the custom plugin. This is useful when custom behavior is needed
 * for processing each entry in the queue.
 */
public final class CustomPluginQueueEntryReader extends AbstractTailerPollingQueueEntryReader {

    private final ChronicleReaderPlugin plugin;  // The custom plugin for processing queue entries
    private final MessageConsumer consumer;  // The message consumer that handles the processed messages

    /**
     * Constructs a {@code CustomPluginQueueEntryReader} with the specified {@link ExcerptTailer},
     * polling method, plugin, and message consumer.
     *
     * @param tailer      The {@link ExcerptTailer} used to read entries from the queue
     * @param pollMethod  The function that determines how to poll for entries from the tailer
     * @param plugin      The custom plugin that processes each queue entry
     * @param consumer    The message consumer that handles the processed messages
     */
    public CustomPluginQueueEntryReader(ExcerptTailer tailer, Function<ExcerptTailer, DocumentContext> pollMethod,
                                        ChronicleReaderPlugin plugin, MessageConsumer consumer) {
        super(tailer, pollMethod);
        this.plugin = plugin;
        this.consumer = consumer;
    }

    /**
     * Reads a document from the queue and processes it using the custom plugin. The plugin
     * is responsible for handling the document content, and it passes the result to the message
     * consumer if applicable.
     *
     * @param documentContext The context of the document being read
     */
    @Override
    protected void doRead(DocumentContext documentContext) {
        // Delegate the document processing to the plugin, passing the result to the message consumer
        plugin.onReadDocument(documentContext, value -> consumer.consume(documentContext.index(), value));
    }
}
