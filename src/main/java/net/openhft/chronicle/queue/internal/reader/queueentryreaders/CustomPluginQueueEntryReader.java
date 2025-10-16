/*
 * Copyright 2016-2025 chronicle.software
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

public final class CustomPluginQueueEntryReader extends AbstractTailerPollingQueueEntryReader {

    private final ChronicleReaderPlugin plugin;
    private final MessageConsumer consumer;

    public CustomPluginQueueEntryReader(ExcerptTailer tailer, Function<ExcerptTailer, DocumentContext> pollMethod, ChronicleReaderPlugin plugin,
                                        MessageConsumer consumer) {
        super(tailer, pollMethod);
        this.plugin = plugin;
        this.consumer = consumer;
    }

    @Override
    protected void doRead(DocumentContext documentContext) {
        plugin.onReadDocument(documentContext, value -> consumer.consume(documentContext.index(), value));
    }
}
