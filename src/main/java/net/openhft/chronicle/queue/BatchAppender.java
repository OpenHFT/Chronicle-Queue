package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;

/**
 * Created by Rob Austin
 *
 * This API is and Advanced API that should be used with care, an only used if you are exactly
 * sure you know what you are doing, miss use of this API could cause the JVM or your application.
 *
 *
 * You should only consider this API if :
 * - you have a batch of messages that you wish to write to a chronicle queue and  you wish to write them directly to the off heap memory
 * - you only have a single appender thread
 * - you dont care about queue roll [ in other words this API wont take account of Queue Roll]
 *
 * before writing messages directly to off heap memory checks have to be made to ensure that the batch fits into the existing block, or that an individual message does not have to be indexed. There is more on this below.
 * When you come to write each message you must start by skipping 4 bytes in other words leaving them as byte[]{0,0,0,0} [ which will become the length later ( as a java int ) ], first write the data, then go back and set the 4 byte length, the data must be written first and then the length to ensure that a tailer does not attempt to read a half written message.
 *
 * The single threaded appending thread must make sure that it does not exceed any of of the
 * conditions below ( see java doc below for more detail ), periodically it will have to call
 * into chronicle queues appender, this is because some messages are indexes or future message
 * may be written to a different block of off heap memory
 */
public interface BatchAppender {

    /**
     * @return the maximum number of message that can be written directly to the off heap memory before
     * * calling using the chronicle queue appender, this is based on the indexing used.
     */
    int rawMaxMessage();

    /**
     * @return the maximum number of bytes that can be written directly to the off heap memory
     * before
     * * calling using the chronicle queue appender, this is based on the block size used.
     */
    int rawMaxBytes();

    /**
     * @return the address of the next message
     */
    long rawAddress();

    /**
     * you should make this call at the end of each batch, the reason for this call, is that the
     * next message must be written by chronicle-queue, either because the next message requires
     * indexing or the next message has to be written to
     *
     * @param bytes            the data to be written to the chronicle-queue
     * @param address          the address of the last message written.
     * @param numberOfMessages the number of messages that where written in the last batch
     */
    void update(Bytes bytes, long address, long numberOfMessages);

}
