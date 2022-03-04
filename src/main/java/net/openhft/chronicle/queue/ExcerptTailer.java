/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
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
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableIn;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.SourceContext;
import org.jetbrains.annotations.NotNull;

/**
 * The component that facilitates sequentially reading data from a {@link ChronicleQueue}.
 *
 * <p><b>NOTE:</b> Tailers are NOT thread-safe, sharing a Tailer between threads will lead to errors and unpredictable behaviour.</p>
 */
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
     * @param includeMetaData if the DocumentContext shall be meta data aware.
     *
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
     *
     */
    @Override
    long index();
    default long lastReadIndex() { return -1; }

    /**
     * Returns the current cycle for this Tailer.
     * <p>
     * Usually, each cycle will have its own unique data file to store excerpts.
     *
     * @return Returns the current cycle for this Tailer
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
     * new millisecond. Hence a whole milliseconds with of data could be added to the
     * chronicle-queue that toEnd() won’t see. For appenders that that are using the same queue
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
     *
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
     * @throws NullPointerException if the provide {@code direction} is {@code null}
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
     * Winds this ExcerptTailer to after the last entry which wrote an entry to the queue.
     *
     * @param queue which was written to.
     * @return this ExcerptTailer
     *
     * @throws IORuntimeException if the provided {@code queue} couldn't be wound to the last index.
     * @throws NullPointerException if the provided {@code queue} is {@code null}
     */
    @NotNull
    ExcerptTailer afterLastWritten(ChronicleQueue queue);

    /**
     * Sets the Read After Replica Acknowledged property of this Tailer to the
     * provided {@code readAfterReplicaAcknowledged}.
     * <p>
     * Enterprise Queue only: if replication enabled, setting this to <code>true</code> on a source queue ensures that
     * this tailer will not read until at least one of the sinks has acknowledged receipt of the excerpt.
     * This will block forever if no sinks acknowledge receipt.
     *
     * @param readAfterReplicaAcknowledged enable
     */
    default void readAfterReplicaAcknowledged(boolean readAfterReplicaAcknowledged) {
    }

    /**
     * Returns the Read After Replica Acknowledged property of this Tailer.
     * <p>
     * Enterprise Queue only: if replication enabled, setting this to <code>true</code> on a source queue ensures that
     * this tailer will not read until at least one of the sinks has acknowledged receipt of the excerpt.
     * This will block forever if no sinks acknowledge receipt.
     *
     * @return the Read After Replica Acknowledged property of this Tailer
     */
    default boolean readAfterReplicaAcknowledged() {
        return false;
    }

    /**
     * Returns the {@link TailerState} of this Tailer.
     *
     * @return the {@link TailerState} of this Tailer
     */
    @NotNull
    TailerState state();

    // Need to add this here until we can release Ring. https://github.com/ChronicleEnterprise/Chronicle-Ring/issues/12
    @Override
    default @NotNull ExcerptTailer disableThreadSafetyCheck(boolean disableThreadSafetyCheck) {
        return this;
    }
}
