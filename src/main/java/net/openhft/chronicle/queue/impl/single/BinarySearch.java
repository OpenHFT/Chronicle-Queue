/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;

/**
 * The {@code BinarySearch} class provides functionality to perform a binary search
 * across excerpts within a Chronicle Queue using {@link Wire} as the key and a {@link Comparator}
 * for matching.
 * <p>
 * This implementation relies on Chronicle Queue's encoded 64-bit indexes. While it works under current
 * assumptions where the high bit of the index is not set, it may become unreliable if that changes in future.
 */
public enum BinarySearch {
    INSTANCE;

    /**
     * Performs a binary search using the provided key and comparator.
     * It searches for an exact match and returns the index if found, or an approximate index in the form of {@code -approximateIndex}.
     * Returns {@code -1} if no search is possible or no match is found.
     * <p>
     * Warning : This implementation is unreliable as index are an encoded 64bits, where we could use all the bits including the
     * high bit which is used for the sign. At the moment  it will work as its unlikely to reach a point where we store
     * enough messages in the chronicle queue to use the high bit, having said this its possible in the future the
     * high bit in the index ( used for the sign ) may be used, this implementation is unsafe as it relies on this
     * bit not being set ( in other words set to zero ).
     *
     * @param tailer The {@link ExcerptTailer} used to read from the queue.
     * @param key    The {@link Wire} key used for comparison.
     * @param c      The {@link Comparator} to compare entries.
     * @return The index if an exact match is found, otherwise {@code -approximateIndex} or {@code -1} if no match is found.
     */
    public static long search(@NotNull ExcerptTailer tailer,
                              @NotNull Wire key,
                              @NotNull Comparator<Wire> c) {
        final long readPosition = key.bytes().readPosition();
        try {
            final long start = tailer.toStart().index();
            final long end = tailer.toEnd().index();

            final RollCycle rollCycle = tailer.queue().rollCycle();
            final int startCycle = rollCycle.toCycle(start);
            final int endCycle = rollCycle.toCycle(end);

            if (startCycle == endCycle)
                return findWithinCycle(key, c, startCycle, tailer, rollCycle);

            final NavigableSet<Long> cycles = ((SingleChronicleQueue) tailer.queue()).listCyclesBetween(startCycle, endCycle);
            final int cycle = (int) findCycleLinearSearch(cycles, key, c, tailer);

            if (cycle == -1)
                return -1;
            return findWithinCycle(key, c, cycle, tailer, rollCycle);
        } finally {
            key.bytes().readPosition(readPosition);
        }
    }

    /**
     * Performs a linear search through the available cycles to find the one that may contain the key.
     *
     * @param cycles The set of available cycles.
     * @param key    The key to search for.
     * @param c      The comparator for comparing keys.
     * @param tailer The tailer used for reading the queue.
     * @return The found cycle or the previous cycle if no exact match was found.
     */
    private static long findCycleLinearSearch(@NotNull NavigableSet<Long> cycles, Wire key,
                                              @NotNull Comparator<Wire> c,
                                              @NotNull ExcerptTailer tailer) {
        long readPosition = key.bytes().readPosition();

        final Iterator<Long> iterator = cycles.iterator();
        if (!iterator.hasNext())
            return -1;
        final RollCycle rollCycle = tailer.queue().rollCycle();
        long prevIndex = iterator.next();

        cycleLoop:
        while (iterator.hasNext()) {

            final Long current = iterator.next();

            final boolean b = tailer.moveToIndex(rollCycle.toIndex((int) (long) current, 0));
            if (!b)
                return prevIndex;

            while (true) {
                try (final DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        return prevIndex;
                    }
                    if (rollCycle.toCycle(dc.index()) > current) {
                        continue cycleLoop;
                    }
                    try {
                        final int compare = c.compare(dc.wire(), key);
                        if (compare == 0)
                            return current;
                        else if (compare > 0)
                            return prevIndex;
                        prevIndex = current;
                        break;
                    } catch (NotComparableException e) {
                        // Keep scanning forward
                    } finally {
                        key.bytes().readPosition(readPosition);
                    }
                }
            }
        }
        return prevIndex;
    }

    /**
     * Finds an entry within the cycle that matches the key using binary search.
     *
     * @param key       The key to search for.
     * @param c         The comparator for comparing keys.
     * @param cycle     The cycle to search within.
     * @param tailer    The tailer used to navigate the queue.
     * @param rollCycle The roll cycle information for the queue.
     * @return The index if an exact match is found, an approximation in the form of -approximateIndex
     * or a negative number (- the approx index) if there was no searching to be done.
     * <p>
     * Warning : This implementation is unreliable as index are an encoded 64bits, where we could use all the bits including the
     * high bit which is used for the sign. At the moment  it will work as its unlikely to reach a point where we store
     * enough messages in the chronicle queue to use the high bit, having said this its possible in the future the
     * high bit in the index ( used for the sign ) may be used, this implementation is unsafe as it relies on this
     * bit not being set ( in other words set to zero ).
     */
    public static long findWithinCycle(@NotNull Wire key,
                                       @NotNull Comparator<Wire> c,
                                       int cycle,
                                       @NotNull ExcerptTailer tailer,
                                       @NotNull final RollCycle rollCycle) {
        final long readPosition = key.bytes().readPosition();
        try {
            long lowSeqNum = 0;

            long highSeqNum = tailer.excerptsInCycle(cycle) - 1;

            // nothing to search
            if (highSeqNum < lowSeqNum)
                return -1;

            long midIndex = 0;

            while (lowSeqNum <= highSeqNum) {
                long midSeqNumber = (lowSeqNum + highSeqNum) >>> 1L;

                midIndex = rollCycle.toIndex(cycle, midSeqNumber);

                final boolean b = tailer.moveToIndex(midIndex);
                assert b;
                while (true) {
                    try (DocumentContext dc = tailer.readingDocument()) {
                        if (!dc.isPresent())
                            return -1;
                        key.bytes().readPosition(readPosition);
                        try {
                            int cmp = c.compare(dc.wire(), key);

                            if (cmp < 0) {
                                lowSeqNum = rollCycle.toSequenceNumber(dc.index()) + 1;
                                break;
                            } else if (cmp > 0) {
                                highSeqNum = midSeqNumber - 1;
                                break;
                            } else {
                                return dc.index(); // key found
                            }
                        } catch (NotComparableException e) {
                            // We reached the upper bound, eliminate the top half of the range
                            if (rollCycle.toSequenceNumber(dc.index()) == highSeqNum) {
                                highSeqNum = midSeqNumber - 1;
                                break;
                            }
                        }
                    }
                }
            }

            return midIndex == 0 ? -1 : -midIndex; // Return -approximateIndex if not exact match
        } finally {
            key.bytes().readPosition(readPosition);
        }
    }
}
