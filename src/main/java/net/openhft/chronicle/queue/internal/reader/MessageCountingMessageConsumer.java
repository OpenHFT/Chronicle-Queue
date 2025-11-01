/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.queue.reader.MessageConsumer;

/**
 * A message consumer that counts the messages passed to a wrapped
 * consumer that made it to the sink
 */
public final class MessageCountingMessageConsumer implements MessageConsumer {
    private final long matchLimit;
    private final MessageConsumer wrappedConsumer;
    private long matches = 0;

    /**
     * Constructs a {@code MessageCountingMessageConsumer} with the specified match limit and wrapped consumer.
     *
     * @param matchLimit      The maximum number of messages to consume before stopping. A value of 0 means no limit.
     * @param wrappedConsumer The downstream consumer that processes the messages
     */
    public MessageCountingMessageConsumer(long matchLimit, MessageConsumer wrappedConsumer) {
        this.matchLimit = matchLimit;
        this.wrappedConsumer = wrappedConsumer;
    }

    /**
     * Consumes a message by passing it to the wrapped consumer. If the wrapped consumer processes the message,
     * the match counter is incremented.
     *
     * @param index   The index of the message
     * @param message The message content
     * @return {@code true} if the message was consumed, {@code false} otherwise
     */
    @Override
    public boolean consume(long index, String message) {
        final boolean consume = wrappedConsumer.consume(index, message);
        if (consume) {
            matches++;
        }
        return consume;
    }

    /**
     * Checks if the match limit has been reached.
     *
     * @return {@code true} if the number of consumed messages equals or exceeds the match limit, {@code false} otherwise
     */
    public boolean matchLimitReached() {
        return matchLimit > 0 && matches >= matchLimit;
    }
}
