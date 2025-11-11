/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

    private final ExcerptTailer tailer;
    private final Function<ExcerptTailer, DocumentContext> pollMethod;
    private final QueueEntryHandler messageConverter;
    private final MessageConsumer messageConsumer;

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
                return false;
            }

            messageConverter.accept(dc.wire(), val -> messageConsumer.consume(dc.index(), val));
            return true;
        }
    }
}
