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
