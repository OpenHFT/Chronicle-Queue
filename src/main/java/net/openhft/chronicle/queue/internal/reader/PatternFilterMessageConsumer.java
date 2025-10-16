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

import java.util.List;
import java.util.regex.Pattern;

/**
 * A MessageConsumer that only passes messages through that do or
 * do not match a list of patterns
 */
public final class PatternFilterMessageConsumer implements MessageConsumer {

    private final List<Pattern> patterns;
    private final boolean shouldBePresent;
    private final MessageConsumer nextMessageConsumer;

    /**
     * Constructor
     *
     * @param patterns            The list of patterns to match against
     * @param shouldBePresent     true if we require all the patterns to match, false if we require none of the patterns to match
     * @param nextMessageConsumer The next message consumer in line, messages that pass the filter will be passed to it
     */
    public PatternFilterMessageConsumer(List<Pattern> patterns, boolean shouldBePresent, MessageConsumer nextMessageConsumer) {
        this.patterns = patterns;
        this.shouldBePresent = shouldBePresent;
        this.nextMessageConsumer = nextMessageConsumer;
    }

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
