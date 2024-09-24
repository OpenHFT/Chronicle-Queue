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

package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.queue.reader.MessageConsumer;

/**
 * {@code MessageCountingMessageConsumer} is a wrapper for a {@link MessageConsumer} that counts
 * how many messages have been passed to the consumer and checks whether a defined match limit has been reached.
 * <p>
 * This class is useful for scenarios where processing should stop after a certain number of messages have been consumed.
 */
public final class MessageCountingMessageConsumer implements MessageConsumer {
    private final long matchLimit;  // The maximum number of messages to consume before stopping
    private final MessageConsumer wrappedConsumer;  // The underlying message consumer to which messages are passed
    private long matches = 0;  // Counter for how many messages have been consumed

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
            matches++;  // Increment match count if the message was consumed
        }
        return consume;
    }

    /**
     * Checks if the match limit has been reached.
     *
     * @return {@code true} if the number of consumed messages equals or exceeds the match limit, {@code false} otherwise
     */
    public boolean matchLimitReached() {
        return matchLimit > 0 && matches >= matchLimit;  // Return true if match limit is reached
    }
}
