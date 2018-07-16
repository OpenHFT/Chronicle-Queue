package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;

/**
 * Created by Rob Austin
 *
 * This API is and Advanced Chronicle Queue API that should be used with care, an only used if you
 * are sure you know what you are doing, misuse of this API could corrupt you data or even
 * worst cause the JVM or your application to crash.
 *
 *
 * You should only consider this API if :
 * - you have a batch of messages that you wish to write to a chronicle queue and you wish to write them directly to the off heap memory
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
 * When you come to write each message you must start by skipping 4 bytes in other words leaving them as byte[]{0,0,0,0} [ which will become the length later ( as a java int ) ], first write the data, then go back and set the 4 byte length, the data must be written first and then the length to ensure that a tailer does not attempt to read a half written message.
 *
 * The appending thread must make sure that it does not exceed the rawMaxMessage() or rawMaxBytes().
 * If no more can data be written to the off heap memory, then the next call must be to:
 *
 * {@link net.openhft.chronicle.queue.BatchAppender#update(net.openhft.chronicle.bytes.Bytes, long, long)}
 *
 * This is because periodically, some messages must be indexed or written to a different block of
 * off heap memory
 */
public interface BatchAppender {

    /**
     * @return the maximum number of messages that can be written directly to the off heap memory
     * before calling {@link net.openhft.chronicle.queue.BatchAppender#update(net.openhft.chronicle.bytes.Bytes, long, long)}, this is based on the indexing used.
     */
    int rawMaxMessage();

    /**
     * @return the maximum number of bytes that can be written directly to the off heap memory
     * before calling {@link net.openhft.chronicle.queue.BatchAppender#update(net.openhft.chronicle.bytes.Bytes, long, long)}, this is based on the block size used.
     */
    int rawMaxBytes();

    /**
     * @return the address of the next message
     */
    long rawAddress();

    /**
     * you should make this call at the end of each batch, the reason for this call, is that the
     * next message must be written by chronicle-queue, either because the next message requires
     * indexing or the next message has to be written to a different block, if you can not
     * complete writing a full message + the 4 byte len directly to the off heap memory then you
     * shuld call this method instead.
     *
     * @param bytes            the data to be written to the chronicle-queue
     * @param address          the address of the last message written.
     * @param numberOfMessages the number of messages that where written in the last batch
     */
    void update(Bytes bytes, long address, long numberOfMessages);

}
