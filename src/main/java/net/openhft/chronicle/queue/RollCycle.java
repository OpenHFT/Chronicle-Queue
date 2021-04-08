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

import net.openhft.chronicle.core.time.TimeProvider;
import org.jetbrains.annotations.NotNull;

public interface RollCycle {

    /**
     * Returns the format that is to be applied when file names are calculated for a new roll cycle.
     * <p>
     * For example, the following formats can be returned:
     * <ul>
     *     <li>"yyyyMMdd-HHmm" (MINUTELY)</li>
     *     <li>"yyyyMMdd" (DAILY)</li>
     * </ul>
     *
     * @return the format that is to be applied when file names are calculated for a new roll cycle
     */
    @NotNull
    String format();

    /**
     * Returns the length in milliseconds (i.e. the maximum duration) for a roll cycle.
     * <p>
     * For example, the following lengths can be returned:
     * <ul>
     *     <li>60 * 1,000 = 60,000 (MINUTELY)</li>
     *     <li>24 * 60 * 60 * 1,000 = 86,400,000 (DAILY)</li>
     * </ul>
     *
     * @return the length in milliseconds (i.e. the maximum duration) for a roll cycle
     */
    int lengthInMillis();

    @Deprecated(/* to be removed in x.22 */)
    default int length() {
        return lengthInMillis();
    }

    /**
     * @return the size of each index array, note: indexCount^2 is the maximum number of index queue entries.
     */
    int defaultIndexCount();

    /**
     * Returns the space between excerpts that are explicitly indexed.
     * <p>
     * A higher number means higher sequential write performance but slower random access read. The sequential read performance is not affected by
     * this property.
     * <p>
     * For example, the following default index spacing can be returned:
     * <ul>
     *     <li>16 (MINUTELY)</li>
     *     <li>64 (DAILY)</li>
     * </ul>
     *
     * @return the space between excerpts that are explicitly indexed
     */
    int defaultIndexSpacing();

    /**
     * @param epoch and EPOCH offset, to all the user to define their own epoch
     * @return the cycle
     */
    int current(TimeProvider time, long epoch);

    /**
     * Returns the index for the given {@code cycle} and {@code sequenceNumber}.
     * <p>
     * An index is comprised of both a cycle and a sequence number but the way the index is composed of said properties may vary.
     *
     * @param cycle          to be composed into an index
     * @param sequenceNumber to be composed into an index
     * @return the index for the given {@code cycle} and {@code sequenceNumber}
     */
    long toIndex(int cycle, long sequenceNumber);

    /**
     * Returns the sequence number for the given {@code index}.
     * <p>
     * An index is comprised of both a cycle and a sequence number but the way the index is composed of said properties may vary. This method
     * decomposes the provided {@code index} and extracts the sequence number.
     *
     * @param index to use when extracting the sequence number
     * @return the sequence number for the given {@code index}
     */
    long toSequenceNumber(long index);

    /**
     * Returns the cycle for the given {@code index}.
     * <p>
     * An index is comprised of both a cycle and a sequence number but the way the index is composed of said properties may vary. This method
     * decomposes the provided {@code index} and extracts the cycle.
     *
     * @param index to use when extracting the cycle
     * @return the sequence number for the given {@code index}
     */
    int toCycle(long index);

    /**
     * @return the maximum number of messages per cycle
     */
    long maxMessagesPerCycle();

}
