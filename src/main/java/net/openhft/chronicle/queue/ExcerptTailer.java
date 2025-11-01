/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.annotation.SingleThreaded;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableIn;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.SourceContext;
import org.jetbrains.annotations.NotNull;

/**
 * The component that facilitates sequentially reading data from a {@link ChronicleQueue}.
 *
 * <p><b>NOTE:</b> Tailers are NOT thread-safe, sharing a Tailer between threads will lead to errors and unpredictable behaviour.
 */
@SingleThreaded
public interface ExcerptTailer extends ExcerptCommon<ExcerptTailer>, MarshallableIn, SourceContext {

    /**
     * Returns the {@link DocumentContext } for this ExcerptTailer.
     * <p>
     * This is equivalent to {@link  ExcerptTailer#readDocument(ReadMarshallable)} but without the use of a
     * lambda expression.
     * <p>
     * This method is the ExcerptTailer equivalent of {@link net.openhft.chronicle.wire.WireIn#readingDocument()}
     *
     * @return the document context
     */
    @Override
    @NotNull
    default DocumentContext readingDocument() {
        return readingDocument(false);
    }

    /**
     * Returns the {@link DocumentContext } for this ExcerptTailer.
     * <p>
     * This is equivalent to {@link  ExcerptTailer#readDocument(ReadMarshallable)} but without the use of a
     * lambda expression.
     * <p>
     * This method is the ExcerptTailer equivalent of {@link net.openhft.chronicle.wire.WireIn#readingDocument()}
     *
     * @param includeMetaData if the DocumentContext shall be metadata aware.
     * @return the document context
     */
    @NotNull
    DocumentContext readingDocument(boolean includeMetaData);

    /**
     * Returns the current index of this Tailer.
     * <p>
     * If this method is invoked within a {@code try (tailer.readingDocument(){ }} block, returns the index of
     * the current reading document. Otherwise, returns the next index to read.
     * <p>
     * The index includes the cycle and the sequence number within that cycle.
     *
     * @return the current index of this Tailer
     */
    @Override
    long index();

    default long lastReadIndex() {
        return -1;
    }

    /**
     * Returns the current cycle for this Tailer.
     * <p>
     * Usually, each cycle will have its own unique data file to store excerpts.
     *
     * @return Returns the current cycle for this Tailer, or Integer.MIN_VALUE if no cycle has been loaded.
     */
    int cycle();

    /**
     * Tries to move the index for this Tailer to the provided {@code index}.
     * <p>
     * The index contains both the cycle number and sequence number within the cycle.
     * <p>
     * In order for the operation to succeed, the roll file, corresponding to
     * the cycle number in the index, must be present and the roll file must
     * contain the sequence number contained in the index.
     * <p>
     * If the index is not a valid index, the operation is undefined.
     *
     * @param index to move to.
     * @return if this is a valid index.
     */
    boolean moveToIndex(long index);

    /**
     * Tries to move to the start of a cycle for this Tailer to the provided {@code cycle}.
     * <p>
     * In order for the operation to succeed, the roll file, corresponding to
     * the cycle number, must be present.
     * <p>
     * If the cycle is not a valid cycle, the operation is undefined.
     *
     * @param cycle to move to.
     * @return if this is a valid cycle.
     */
    boolean moveToCycle(int cycle);

    /**
     * Moves the index for this Tailer to the first existing excerpt in the queue.
     *
     * @return this ExcerptTailer
     */
    @NotNull
    ExcerptTailer toStart();

    /**
     * Moves the index for this Tailer to the end of the queue.
     * <p>
     * If the direction() == FORWARD, this will be the index position corresponding to one more
     * than the last entry. Otherwise, the index will be the last excerpt.
     * <p>
     * This is not atomic with the appenders, in other words if a cycle has been added in the
     * current millisecond, toEnd() may not see it, This is because for performance reasons, the
     * queue.lastCycle() is cached, as finding the last cycle is expensive, it requires asking the
     * directory for the Files.list() so, this cache is only refreshed if the call toEnd() is in a
     * new millisecond. Hence, a whole milliseconds with of data could be added to the
     * chronicle-queue that toEnd() won’t see. For appenders that are using the same queue
     * instance ( and with then same JVM ), they can be informed that the last cycle has
     * changed, this will yield better results, but atomicity can still not be guaranteed.
     *
     * @return this ExcerptTailer
     */
    @NotNull
    ExcerptTailer toEnd();

    /**
     * Sets the {@code striding} property of this Tailer.
     * <p>
     * When striding is enabled AND direction is BACKWARD, skip to the entries easiest to find, doesn't need to be every entry.
     *
     * @param striding skip to the indexStride if that is easy, doesn't always happen.
     * @return this ExcerptTailer
     */
    ExcerptTailer striding(boolean striding);

    /**
     * Returns the striding property of this Tailer.
     *
     * @return the striding property of this Tailer
     * @see #striding(boolean)
     */
    boolean striding();

    /**
     * Sets the direction of this ExcerptTailer.
     * <p>
     * The direction determines the direction of movement upon reading an excerpt.
     *
     * @param direction which is either of NONE, FORWARD, BACKWARD
     * @return this ExcerptTailer
     * @throws NullPointerException if the provided {@code direction} is {@code null}
     */
    @NotNull
    ExcerptTailer direction(@NotNull TailerDirection direction);

    /**
     * Returns the direction of this ExcerptTailer.
     * <p>
     * The direction determines the direction of movement upon reading an excerpt.
     *
     * @return the direction of this ExcerptTailer
     */
    TailerDirection direction();

    /**
     * Sets the Read After Replica Acknowledged property of this Tailer to the
     * provided {@code readAfterReplicaAcknowledged}.
     * <p>
     * Enterprise Queue only: if replication enabled, setting this to {@code true} on a source queue ensures that
     * this tailer will not read until at least one of the sinks has acknowledged receipt of the excerpt.
     * This will block forever if no sinks acknowledge receipt.
     *
     * @param readAfterReplicaAcknowledged enable
     */
    default void readAfterReplicaAcknowledged(boolean readAfterReplicaAcknowledged) {
    }

    /**
     * Allows you to control the number of in flight messages that are allowed when setting {@code acknowledgedIndexReplicatedCheck},  By default, this is set to:
     * <p>
     * By providing a custom implementation of {@code acknowledgedIndexReplicatedCheck} you can control the number of in-flight messages that are allowed before the message is available to be read by the tailer.
     * And in addition, then acknowledgedIndexReplicatedCheck will be set to true.
     * <p>
     * below is the default implementation:
     * <pre>
     * boolean acknowledgedIndexReplicatedCheck(long index, long lastSequenceAck) {
     *      return index == lastSequenceAck;
     * }
     * </pre>
     * <p>
     * With this default implementation it will ensure tailer will not be able to read a message until it has been acknowledged that it has been full replicated.
     * <p>
     * However, if you wish to tolerate a number of in-flight or pending messages before the message.,
     * You can set this to a different implementation. For example, if you wish to tolerate one unacknowledged message, set {@code tolerateNumberOfUnAckedMessages} to 1, as shown below:
     * <pre>
     * boolean acknowledgedIndexReplicatedCheck(long index, long lastSequenceAck) {
     *     int tolerateNumberOfUnAckedMessages = 1;
     *     return index &lt;= lastSequenceAck + tolerateNumberOfUnAckedMessages;
     * }
     * </pre>
     * <p>
     * This feature is exclusive to Enterprise Queue and is disabled when accessing a queue file role.
     * Specifically, it's unavailable when the in-flight message originated from a cycle other than the current one.
     * For more information about cycles, see RollCycles and {@link RollCycle#toCycle(long)}.
     *
     * @param acknowledgedIndexReplicatedCheck as a custom implementation of {@code acknowledgedIndexReplicatedCheck}
     */
    default void acknowledgedIndexReplicatedCheck(AcknowledgedIndexReplicatedCheck acknowledgedIndexReplicatedCheck) {
        throw new UnsupportedOperationException("This method is not supported by this implementation");
    }

    /**
     * Returns the Read After Replica Acknowledged property of this Tailer.
     * <p>
     * Enterprise Queue only: if replication enabled, setting this to {@code true} on a source queue ensures that
     * this tailer will not read until at least one of the sinks has acknowledged receipt of the excerpt.
     * This will block forever if no sinks acknowledge receipt.
     *
     * @return the Read After Replica Acknowledged property of this Tailer
     */
    default boolean readAfterReplicaAcknowledged() {
        return false;
    }

    /**
     * Return the exact number of excerpts in a cycle available for reading.
     * <p>
     * Calling this method may move ExcerptTailer to the specified cycle and release its store.
     *
     * @return the exact number of excerpts in a cycle.
     */
    default long excerptsInCycle(int cycle) {
        try (ExcerptTailer tailer = queue().createTailer()) {
            return tailer.excerptsInCycle(cycle);
        }
    }

    /**
     * Returns the {@link TailerState} of this Tailer.
     *
     * @return the {@link TailerState} of this Tailer
     */
    @NotNull
    TailerState state();

    interface AcknowledgedIndexReplicatedCheck {
        /**
         * @param index           the index of the next message for the tailer to read
         * @param lastSequenceAck the last index that has been acknowledged as replicated
         * @return true if you are ok for the tailer to read the message at {@code index}
         * <p>
         * NOTE: this callback is not called during a roll,
         * as all messages must be acknowledged before they are seen during this period.
         */
        boolean acknowledgedIndexReplicatedCheck(long index, long lastSequenceAck);
    }
}
