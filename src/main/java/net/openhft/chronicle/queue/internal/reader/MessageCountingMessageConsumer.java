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
     * Constructor
     *
     * @param matchLimit      The limit used to determine {@link #matchLimitReached()}
     * @param wrappedConsumer The downstream consumer to pass messages to
     */
    public MessageCountingMessageConsumer(long matchLimit, MessageConsumer wrappedConsumer) {
        this.matchLimit = matchLimit;
        this.wrappedConsumer = wrappedConsumer;
    }

    @Override
    public boolean consume(long index, String message) {
        final boolean consume = wrappedConsumer.consume(index, message);
        if (consume) {
            matches++;
        }
        return consume;
    }

    public boolean matchLimitReached() {
        return matchLimit > 0 && matches >= matchLimit;
    }
}
