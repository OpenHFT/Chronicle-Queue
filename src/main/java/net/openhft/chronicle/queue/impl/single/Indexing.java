/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
     * Get the sequence number of the last entry present in the cycle.
     * <p>
     * Note: If you're not holding the write lock when you call this and there are concurrent writers,
     * the value may be stale by the time it's returned. If you're holding the write lock it is guaranteed
     * to be accurate.
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
