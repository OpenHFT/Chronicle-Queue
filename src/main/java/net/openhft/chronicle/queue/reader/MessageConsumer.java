/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.reader;

/**
 * Message consumers make a chain-of-responsibility pattern, they
 * can filter or transform the input. The sink will be the final
 * consumer in the pipeline.
 */
public interface MessageConsumer {

    /**
     * Consumes the given message, performing any filtering or transformation as necessary.
     *
     * @param index   The index of the message within the queue
     * @param message The message content as a string
     * @return {@code true} if the message was passed through to the sink, {@code false} if it was filtered out
     */
    boolean consume(long index, String message);
}
