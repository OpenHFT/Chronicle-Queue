/*
 * Copyright 2016-2022 chronicle.software
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

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.ExcerptContext;

import java.io.StreamCorruptedException;

/**
 * The {@code Indexing} interface provides internal-only indexing functionality for
 * {@link SingleChronicleQueue}. This interface supports indexing operations like tracking
 * the next entry to be indexed, verifying indexability, and retrieving sequence numbers.
 * It is designed to be expanded over time to further clarify the API contract of {@link SCQIndexing}.
 */
public interface Indexing {

    /**
     * Retrieves the number of entries between each index.
     *
     * @return the number of entries between each index.
     */
    int indexCount();

    /**
     * Retrieves the spacing between indexed entries.
     * If the spacing is set to 1, every entry will be indexed.
     *
     * @return the spacing between indexed entries.
     */
    int indexSpacing();

    /**
     * Returns the next entry to be indexed in the queue.
     *
     * @return the index of the next entry to be indexed.
     */
    long nextEntryToBeIndexed();

    /**
     * Checks if a given index is indexable based on the current indexing strategy.
     *
     * @param index The index to check.
     * @return {@code true} if the index is eligible for indexing, otherwise {@code false}.
     */
    boolean indexable(long index);

    /**
     * Retrieves the sequence number of the last entry present in the cycle.
     * <p>
     * <b>Note:</b> If not holding a write lock, this value may be stale due to concurrent writers.
     * If holding the write lock, it is guaranteed to be accurate.
     *
     * @param ex An {@link ExcerptContext} used to scan the roll cycle if necessary.
     * @return the sequence number of the last excerpt in the cycle.
     * @throws StreamCorruptedException if the index is corrupted.
     */
    long lastSequenceNumber(ExcerptContext ex) throws StreamCorruptedException;

    /**
     * Retrieves the count of linear scans that have occurred while indexing.
     *
     * @return the number of linear scans performed.
     */
    int linearScanCount();

    /**
     * Retrieves the count of linear scans performed based on positions.
     *
     * @return the number of position-based linear scans.
     */
    int linearScanByPositionCount();
}
