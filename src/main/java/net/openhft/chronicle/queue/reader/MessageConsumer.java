package net.openhft.chronicle.queue.reader;

/**
 * Message consumers make a chain-of-responsibility pattern, they
 * can filter or transform the input. The sink will be the final
 * consumer in the pipeline.
 */
public interface MessageConsumer {

    /**
     * Consume the message
     *
     * @param index   the index of the message
     * @param message the message as a string
     * @return True if the message was sent through to the sink, false if it was filtered
     */
    boolean consume(long index, String message);
}
