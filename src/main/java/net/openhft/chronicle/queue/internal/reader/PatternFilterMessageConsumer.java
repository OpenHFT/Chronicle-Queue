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
