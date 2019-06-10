/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
 * <p>The component that facilitates sequentially reading data from a {@link ChronicleQueue}.</p>
 * <p><b>NOTE:</b> Tailers are NOT thread-safe, sharing the Tailer between threads will lead to errors and unpredictable behaviour.</p>
 *
 * @author peter.lawrey
 */
public interface ExcerptTailer extends ExcerptCommon<ExcerptTailer>, MarshallableIn, SourceContext {

    /**
     * equivalent to {@link  ExcerptTailer#readDocument(ReadMarshallable)} but with out the use of a
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

    @NotNull
    DocumentContext readingDocument(boolean includeMetaData);

    /**
     * peekDocument() can be used after a message has been found by toStart() or readingDocument().
     * Until then only readingDocument() will find the first cycle.
     *
     * @return true if readingDocument() should be called, false if most likely it's not needed.
     */
    default boolean peekDocument() {
        return true;
    }

    /**
     * @return if called while within the <code>try (tailer.readingDocument){ }</code> block, returns index of current reading document.
     * Otherwise, the next index to read.
     * Index includes the cycle and the sequence number in that cycle
     */
    @Override
    long index();

    /**
     * @return the cycle this tailer is on, usually with chronicle-queue each cycle will have its
     * own unique data file to store the excerpt
     */
    int cycle();

    /**
     * Randomly select an Excerpt.
     *
     * @param index index to look up, the index includes the cycle number and a sequence number from with this cycle
     * @return true if this is a valid entries.
     */
    boolean moveToIndex(long index);

    /**
     * Replay from the first entry in the first cycle.
     *
     * @return this Excerpt
     */
    @NotNull
    ExcerptTailer toStart();

    /**
     * Wind to the last entry in the last cycle
     * <p> If the direction() == FORWARD, this will be 1 more than the last entry.
     * <br/>Otherwise the index will be the last entry. </p>
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
     * @return this Excerpt
     */
    @NotNull
    ExcerptTailer toEnd();

    /**
     * @return the direction of movement after reading an entry.
     */
    TailerDirection direction();

    /**
     * Set the direction of movement.
     *
     * @param direction NONE, FORWARD, BACKWARD
     * @return this
     */
    @NotNull
    ExcerptTailer direction(TailerDirection direction);

    /**
     * Wind this tailer to after the last entry which wrote an entry to the queue
     *
     * @param queue which was written to.
     * @return this ExcerptTailer
     * @throws IORuntimeException if the queue couldn't be wound to the last index.
     */
    @NotNull
    ExcerptTailer afterLastWritten(ChronicleQueue queue) throws IORuntimeException;

    default void readAfterReplicaAcknowledged(boolean readAfterReplicaAcknowledged) {
    }

    default boolean readAfterReplicaAcknowledged() {
        return false;
    }

    @NotNull
    TailerState state();

    /**
     * A task that will be run if a WeakReference referring this appender is registered with a clean-up task.
     *
     * @return Task to release any associated resources
     */
    default Runnable getCloserJob() {
        return () -> {
        };
    }
}
