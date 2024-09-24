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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.TimeProvider;
import org.jetbrains.annotations.NotNull;

/**
 * Interface defining the behavior of a roll cycle in Chronicle Queue. A roll cycle dictates how frequently data rolls over
 * to a new file and manages the indexing of records within those cycles.
 * <p>
 * Roll cycles are defined by a period of time, such as MINUTELY or DAILY, and they provide methods for managing the
 * cycle duration, formatting, and indexing.
 */
public interface RollCycle {

    /**
     * Returns the format to be applied when file names are calculated for a new roll cycle.
     * <p>
     * For example, the following formats may be returned:
     * <ul>
     *     <li>"yyyyMMdd-HHmm" (MINUTELY)</li>
     *     <li>"yyyyMMdd" (DAILY)</li>
     * </ul>
     * The lexicographical order of formatted cycles must preserve chronological order, meaning that if {@code cycle1 < cycle2},
     * the same relation must hold true for their string representations.
     *
     * @return the format that is applied when file names are calculated for a new roll cycle
     */
    @NotNull
    String format();

    /**
     * Returns the length in milliseconds for a roll cycle, representing the maximum duration for one cycle.
     * <p>
     * Examples of cycle lengths:
     * <ul>
     *     <li>60,000 ms (1 minute) for MINUTELY</li>
     *     <li>86,400,000 ms (1 day) for DAILY</li>
     * </ul>
     *
     * @return the length in milliseconds for the roll cycle
     */
    int lengthInMillis();

    /**
     * Returns the default epoch offset if none is set. The epoch offset allows users to define their own start time for the cycle.
     *
     * @return the default epoch offset, typically 0
     */
    default int defaultEpoch() {
        return 0;
    }

    /**
     * Returns the size of each index array. The value {@code indexCount^2} represents the maximum number of index queue entries.
     *
     * @return the size of each index array
     */
    int defaultIndexCount();

    /**
     * Returns the space between excerpts that are explicitly indexed.
     * A larger value improves sequential write performance but slows down random access reads. Sequential read performance is not impacted.
     * <p>
     * Example values:
     * <ul>
     *     <li>16 (MINUTELY)</li>
     *     <li>64 (DAILY)</li>
     * </ul>
     *
     * @return the space between excerpts that are explicitly indexed
     */
    int defaultIndexSpacing();

    /**
     * Calculates and returns the current cycle based on the current time and epoch offset.
     * The default epoch is 0, so for a DAILY cycle this method will return the number of days since 1970-01-01T00:00:00Z.
     *
     * @param time  The {@link TimeProvider} that supplies the current time
     * @param epoch The epoch offset to be used for the calculation
     * @return the current cycle number
     */
    default int current(@NotNull TimeProvider time, long epoch) {
        return (int) ((time.currentTimeMillis() - epoch) / lengthInMillis()); // Calculate cycle based on time and epoch
    }

    /**
     * Returns the index for the given cycle and sequence number.
     * <p>
     * The index is composed of the cycle shifted left and the sequence number. For instance:
     * <ul>
     *     <li>DAILY cycle: cycleShift=32 and sequenceMask=0xFFFFFFFF, allowing 2^32 entries per day</li>
     *     <li>HUGE_DAILY: cycleShift=48 and sequenceMask=0xFFFFFFFFFFFF, allowing 2^48 entries per day</li>
     * </ul>
     *
     * @param cycle          The cycle to be composed into an index
     * @param sequenceNumber The sequence number to be composed into an index
     * @return the index composed of the cycle and sequence number
     */
    long toIndex(int cycle, long sequenceNumber);

    /**
     * Extracts and returns the sequence number from the given index.
     * <p>
     * The index comprises both a cycle and a sequence number, but the way they are composed may vary. This method
     * decomposes the index and extracts the sequence number.
     *
     * @param index The index to extract the sequence number from
     * @return the sequence number extracted from the index
     */
    long toSequenceNumber(long index);

    /**
     * Extracts and returns the cycle from the given index.
     * <p>
     * The index comprises both a cycle and a sequence number, but the way they are composed may vary. This method
     * decomposes the index and extracts the cycle.
     *
     * @param index The index to extract the cycle from
     * @return the cycle extracted from the index
     */
    int toCycle(long index);

    /**
     * Returns the maximum number of messages that can be stored in a single cycle.
     *
     * @return the maximum number of messages per cycle
     */
    long maxMessagesPerCycle();

    // Constant for maximum index count, used for sanity checking index maximums and counts.
    int MAX_INDEX_COUNT = 32 << 10; // 32 * 1024 = 32K
}
