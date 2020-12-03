package net.openhft.chronicle.queue.batch;

/**
 * Created by Rob Austin
 * <p>
 * This API is and advanced Chronicle Queue API that should be used with care and only used if you
 * are sure you know what you are doing, misuse of this API could corrupt your data or even
 * worst cause the JVM or your application to crash.
 * <p>
 * You should only consider this API if :
 * - you have a batch of messages that you wish to write to a chronicle queue and you wish to write
 * them directly to the off heap memory
 * - you only have a single appender thread
 * - you don't care about queue roll [ in other words this API wont take account of Queue Roll]
 * <p>
 * Before writing messages directly to off heap memory checks have to be made to ensure that the
 * batch fits into the existing memory block, or that an individual message does not have to be
 * indexed. There is more on this below.
 * <p>
 * Writing Each Message
 * --------------------
 * <p>
 * When you come to write each message you must start by skipping 4 bytes in other words leaving
 * them as byte[]{0,0,0,0} [ which will become the length later ( as a java int ) ],
 * first write the data, then go back and set the 4 byte length, the data must be written first
 * and then the length to ensure that a tailer does not attempt to read a half written message.
 */
@FunctionalInterface
@Deprecated() // "to be removed in version 21.x"
public interface BatchAppender {

    /**
     * @param rawAddress     the address of where to start to write a batch of messages to the off heap memory.
     * @param rawMaxBytes    the maximum number of bytes that can be written directly to the off heap memory
     * @param rawMaxMessages the maximum number of messages that can be written directly to the off heap memory
     * @return the count and the length as <code>( count &lt;&lt; 32) | length</code>
     */
    @Deprecated()
    // "to be removed in version 21.x"
    long writeMessages(long rawAddress, long rawMaxBytes, int rawMaxMessages);

}
