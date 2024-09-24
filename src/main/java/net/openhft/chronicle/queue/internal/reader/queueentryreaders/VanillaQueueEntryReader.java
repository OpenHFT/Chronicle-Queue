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
import net.openhft.chronicle.queue.reader.MessageConsumer;
import net.openhft.chronicle.queue.reader.QueueEntryHandler;
import net.openhft.chronicle.queue.reader.QueueEntryReader;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * {@code VanillaQueueEntryReader} is a basic implementation of the {@link QueueEntryReader} interface,
 * responsible for reading entries from a Chronicle queue using a tailer.
 * <p>
 * It converts the read entries using a provided {@link QueueEntryHandler} and forwards the converted
 * message to a {@link MessageConsumer}.
 */
public final class VanillaQueueEntryReader implements QueueEntryReader {

    private final ExcerptTailer tailer;  // The ExcerptTailer used to read entries from the queue
    private final Function<ExcerptTailer, DocumentContext> pollMethod;  // A function for polling the tailer
    private final QueueEntryHandler messageConverter;  // Converts the queue entry into a consumable message
    private final MessageConsumer messageConsumer;  // The consumer that processes the converted messages

    /**
     * Constructs a {@code VanillaQueueEntryReader} with the given tailer, polling method, message converter, and message consumer.
     *
     * @param tailer           The {@link ExcerptTailer} used to read from the queue
     * @param pollMethod       A function that polls the {@link ExcerptTailer} for entries
     * @param messageConverter The {@link QueueEntryHandler} that converts the wire format into a message
     * @param messageConsumer  The {@link MessageConsumer} that consumes the processed message
     */
    public VanillaQueueEntryReader(@NotNull ExcerptTailer tailer, @NotNull Function<ExcerptTailer, DocumentContext> pollMethod,
                                   @NotNull QueueEntryHandler messageConverter, @NotNull MessageConsumer messageConsumer) {
        this.tailer = tailer;
        this.pollMethod = pollMethod;
        this.messageConverter = messageConverter;
        this.messageConsumer = messageConsumer;
    }

    /**
     * Reads a single entry from the queue using the specified polling method and processes it.
     * <p>
     * The entry is converted using the {@link QueueEntryHandler} and passed to the {@link MessageConsumer}.
     *
     * @return {@code true} if an entry was successfully read and processed, {@code false} if no entry was present
     */
    @Override
    public boolean read() {
        try (DocumentContext dc = pollMethod.apply(tailer)) {
            if (!dc.isPresent()) {
                return false;  // No entry available to read
            }

            // Convert the wire format into a consumable message and pass it to the consumer
            messageConverter.accept(dc.wire(), val -> messageConsumer.consume(dc.index(), val));
            return true;
        }
    }
}
