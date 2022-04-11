package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.wire.DocumentContext;

/**
 * Implement this to signal when to halt reading a queue based on the content
 * of a message.
 */
public interface ContentBasedLimiter {

    /**
     * Should the ChronicleReader stop processing messages?
     *
     * @param documentContext The next message to be processed
     * @return true to halt processing before processing the passed message, false otherwise
     */
    boolean shouldHaltReading(DocumentContext documentContext);

    /**
     * Passed to the limiter before processing begins, can be used to provide parameters to it
     * <p>
     * The limiter should make use of {@link Reader#limiterArg()} to convey parameters
     *
     * @param reader The Reader about to be executed
     */
    void configure(Reader reader);
}
