/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.text.ParseException;

/**
 * The {@code RollingChronicleQueue} interface extends the {@link ChronicleQueue} interface and provides
 * additional methods for managing roll cycles, storing and retrieving data, and counting excerpts in a Chronicle Queue.
 *
 * <p>It defines the epoch offset, cycle management, and excerpt counting mechanisms, along with various queue
 * parameters such as index count, spacing, and delta checkpoint intervals.
 */
public interface RollingChronicleQueue extends ChronicleQueue {

    /**
     * Returns the epoch offset of the queue, which is the number of milliseconds since
     * January 1, 1970, 00:00:00 GMT.
     *
     * @return the epoch offset in milliseconds.
     */
    long epoch();

    /**
     * Retrieves the {@link SingleChronicleQueueStore} for a given cycle. Optionally, the store can be created
     * if it doesn't exist.
     *
     * @param cycle          the cycle number to retrieve.
     * @param epoch          the epoch offset in milliseconds since January 1, 1970, 00:00:00 GMT.
     * @param createIfAbsent flag to indicate whether to create the store if it doesn't exist.
     * @param oldStore       the previous store instance, if any.
     * @return the {@code SingleChronicleQueueStore} for the given cycle, or {@code null} if the store doesn't exist and {@code createIfAbsent} is false.
     */
    @Nullable
    SingleChronicleQueueStore storeForCycle(int cycle, final long epoch, boolean createIfAbsent, SingleChronicleQueueStore oldStore);

    /**
     * Finds and returns the first cycle number in the queue.
     *
     * @return the first cycle number, or {@code Integer.MAX_VALUE} if no cycles are found.
     */
    int firstCycle();

    /**
     * Finds and returns the last cycle number available in the queue.
     *
     * @return the last cycle number, or {@code Integer.MIN_VALUE} if no cycles are found.
     */
    int lastCycle();

    /**
     * Counts the total number of excerpts (messages) present in this queue instance.
     *
     * @return the number of document excerpts in the queue.
     */
    long entryCount();

    /**
     * Finds the next available cycle from the current cycle in the specified direction.
     * This method does not create a new cycle if none is available.
     *
     * @param currentCycle the current cycle number.
     * @param direction    the direction in which to search for the next cycle (forward or backward).
     * @return the next available cycle, or {@code -1} if there is no next cycle.
     * @throws ParseException if there is an error parsing the cycle data.
     */
    int nextCycle(int currentCycle, @NotNull TailerDirection direction) throws ParseException;

    /**
     * The number of excerpts between the indexes, {@code fromIndex} inclusive, {@code toIndex}
     * exclusive.
     * <p>
     * When {@code fromIndex} and {@code toIndex} are in different cycles which are not adjacent, this
     * operation can be expensive, as the index count for each intermediate cycle has to be found
     * and calculated. As such, and in this situation, it's not recommended to call this method
     * regularly in latency sensitive systems.
     *
     * @param fromIndex from index, the index provided must exist.  To improve performance no checking
     *                  is carried out to validate if an excerpt exists at this index. ( inclusive )
     * @param toIndex   to index, the index provided must exist. To improve performance no checking is
     *                  carried out to validate if an excerpt exists at this index. ( exclusive )
     * @return the number of excerpts between the indexes, {@code index1} inclusive, {@code index2}
     * exclusive.
     * @throws java.lang.IllegalStateException if the cycle of {@code fromIndex} or {@code toIndex} can
     *                                         not be ascertained
     */
    long countExcerpts(long fromIndex, long toIndex);

    /**
     * Returns the current cycle number.
     *
     * @return the current cycle number.
     */
    int cycle();

    /**
     * Retrieves the maximum size of each index array and the number of index arrays.
     * The maximum number of index queue entries is determined by {@code indexCount}^2.
     *
     * @return the index count, representing the max size and number of index arrays.
     */
    int indexCount();

    /**
     * Retrieves the spacing between indexed entries in the queue.
     * If the spacing is 1, every entry is indexed.
     *
     * @return the spacing between indexed entries.
     */
    int indexSpacing();

    /**
     * Returns the {@link RollCycle} associated with this queue, which defines how the queue rolls over time.
     *
     * @return the roll cycle used by the queue.
     */
    @NotNull
    RollCycle rollCycle();
}
