package net.openhft.chronicle.queue;

/**
 * Created by Rob Austin
 *
 * This API is and advanced Chronicle Queue API that should be used with care and only used if you
 * are sure you know what you are doing, misuse of this API could corrupt your data or even
 * worst cause the JVM or your application to crash.
 *
 *
 * You should only consider this API if :
 * - you have a batch of messages that you wish to write to a chronicle queue and you wish to write
 *      them directly to the off heap memory
 * - you only have a single appender thread
 * - you don't care about queue roll [ in other words this API wont take account of Queue Roll]
 *
 * Before writing messages directly to off heap memory checks have to be made to ensure that the
 * batch fits into the existing memory block, or that an individual message does not have to be
 * indexed. There is more on this below.
 *
 * Writing Each Message
 * --------------------
 *
 * When you come to write each message you must start by skipping 4 bytes in other words leaving
 * them as byte[]{0,0,0,0} [ which will become the length later ( as a java int ) ],
 * first write the data, then go back and set the 4 byte length, the data must be written first
 * and then the length to ensure that a tailer does not attempt to read a half written message.
 *
 * The appending thread must make sure that it does not exceed the rawMaxMessage() or rawMaxBytes().
 * If no more data can be written to the off heap memory, then the next call must be to:
 *
 * {@link BatchAppender#write(long, int, long, long)}
 *
 * This is because periodically, some messages must be indexed or written to a different block of
 * off heap memory
 */
public interface BatchAppender {

    /**
     * @return the maximum number of messages that can be written directly to the off heap memory
     * before calling {@link BatchAppender#write(long, int, long, long)}, this is based on the indexing used.
     */
    int rawMaxMessage();

    /**
     * @return the maximum number of bytes that can be written directly to the off heap memory
     * before calling {@link BatchAppender#write(long, int, long, long)}, this is based on the block size used.
     */
    int rawMaxBytes();

    /**
     * @return the address of where to start to write a batch of messages to the off heap memory.
     */
    long rawAddress();

    /**
     * You should make this call at the end of each batch, in other words if the number of
     * messages  in this batch is now equal to rawMaxMessage() or there is no sufficient space to
     * write any more data based on the rawMaxBytes(). You should also add the 4 byte length to
     * the size of each message.
     * @param sourceBytesAddress            the address of the data to be written to the queue
     * @param sourceByteSize                the size in bytes of the source
     * @param endOfQueueAddress             the address of the last message written to the queue
     * @param numberOfMessagesInLastBatch              the number of messages that where written in the last
     *                                     batch excluding this message.
     */
    void write(long sourceBytesAddress,
               final int sourceByteSize,
               long endOfQueueAddress,
               long numberOfMessagesInLastBatch);

}
