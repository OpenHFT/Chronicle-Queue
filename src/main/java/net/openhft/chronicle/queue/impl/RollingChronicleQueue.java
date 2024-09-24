/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
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
 * parameters such as index count, spacing, and delta checkpoint intervals.</p>
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
     * Calculates the number of excerpts (messages) between two specified index positions.
     *
     * <p>This operation can be expensive if the indexes are in different, non-adjacent cycles,
     * as it may involve querying and calculating index counts for intermediate cycles.</p>
     *
     * @param fromIndex the starting index (inclusive). No validation is performed on this index.
     * @param toIndex   the ending index (exclusive). No validation is performed on this index.
     * @return the number of excerpts between the two indexes.
     * @throws IllegalStateException if the cycle of either {@code fromIndex} or {@code toIndex} cannot be determined.
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

    /**
     * Returns the checkpoint interval used by delta wire. This defines how frequently checkpoints are created.
     *
     * @return the checkpoint interval for delta wire.
     */
    int deltaCheckpointInterval();
}
