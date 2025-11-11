/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

    private final List<Pattern> patterns;
    private final boolean shouldBePresent;
    private final MessageConsumer nextMessageConsumer;

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
            if (shouldBePresent != pattern.matcher(message).find()) {
                return false;
            }
        }
        return nextMessageConsumer.consume(index, message);
    }
}
