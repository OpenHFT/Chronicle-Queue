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

import java.util.List;
import java.util.regex.Pattern;

/**
 * {@code PatternFilterMessageConsumer} is a {@link MessageConsumer} that filters messages based on a list of patterns.
 * <p>
 * It either passes messages that match all the patterns (or none, depending on configuration) to the next consumer
 * in the chain, or filters them out if the pattern conditions are not met.
 * <p>
 * This class can be used for inclusion or exclusion filtering based on regular expressions.
 */
public final class PatternFilterMessageConsumer implements MessageConsumer {

    private final List<Pattern> patterns;  // List of patterns to match against
    private final boolean shouldBePresent;  // True if the patterns should match, false if they should not
    private final MessageConsumer nextMessageConsumer;  // The next consumer in the chain for filtered messages

    /**
     * Constructs a {@code PatternFilterMessageConsumer} with the specified patterns, matching condition,
     * and next consumer.
     *
     * @param patterns            The list of patterns to match against
     * @param shouldBePresent     If {@code true}, all patterns must match; if {@code false}, none of the patterns should match
     * @param nextMessageConsumer The next message consumer in the chain that receives messages passing the filter
     */
    public PatternFilterMessageConsumer(List<Pattern> patterns, boolean shouldBePresent, MessageConsumer nextMessageConsumer) {
        this.patterns = patterns;
        this.shouldBePresent = shouldBePresent;
        this.nextMessageConsumer = nextMessageConsumer;
    }

    /**
     * Consumes a message by checking it against the list of patterns. If the message matches (or doesn't match,
     * depending on {@code shouldBePresent}), it is passed to the next consumer.
     *
     * @param index   The index of the message
     * @param message The message content
     * @return {@code true} if the message was consumed by the next consumer, {@code false} otherwise
     */
    @Override
    public boolean consume(long index, String message) {
        for (Pattern pattern : patterns) {
            // Check if the message matches the pattern, based on the shouldBePresent flag
            if (shouldBePresent != pattern.matcher(message).find()) {
                return false;  // If it doesn't meet the condition, filter out the message
            }
        }
        // Pass the message to the next consumer if all patterns matched (or didn't, depending on the flag)
        return nextMessageConsumer.consume(index, message);
    }
}
