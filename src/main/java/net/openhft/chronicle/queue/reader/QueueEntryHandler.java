/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.queue.internal.reader.InternalMessageToTextQueueEntryHandler;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Handles the processing of queue entries, converting them to text or other forms for consumption.
 * <p>Implements the {@link BiConsumer} interface to consume a {@link WireIn} object, which represents the serialized data,
 * and a {@link Consumer} that processes the resulting string.
 */
public interface QueueEntryHandler extends BiConsumer<WireIn, Consumer<String>>, AutoCloseable {

    /**
     * Closes the handler, releasing any resources held.
     */
    @Override
    void close();

    /**
     * Creates a {@link QueueEntryHandler} that converts messages to text based on the provided {@link WireType}.
     * <p>This is useful when reading queues written in different formats such as binary, JSON, or text.
     *
     * @param wireType The {@link WireType} used to interpret the data
     * @return A {@link QueueEntryHandler} that converts messages to text
     */
    @NotNull
    static QueueEntryHandler messageToText(@NotNull final WireType wireType) {
        return new InternalMessageToTextQueueEntryHandler(wireType);
    }
}
