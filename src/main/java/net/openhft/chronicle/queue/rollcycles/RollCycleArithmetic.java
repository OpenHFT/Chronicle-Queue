/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.rollcycles;

import net.openhft.chronicle.core.Maths;

import static net.openhft.chronicle.queue.RollCycle.MAX_INDEX_COUNT;

/**
 * Reusable and optimized arithmetic for converting between cycles, indices, and sequence numbers
 * in a Chronicle Queue. This class encapsulates logic for indexing and sequencing based on
 * configurable parameters such as {@code indexCount} and {@code indexSpacing}.
 */
public final class RollCycleArithmetic {
    /**
     * Constant representing Sunday, January 4th, 1970 at 00:00:00 UTC, expressed in milliseconds.
     */
    public static final int SUNDAY_00_00 = 259_200_000;
    private final int cycleShift;
    private final int indexCount;
    private final int indexSpacing;
    private final long sequenceMask;

    /**
     * Factory method to create an instance of {@link RollCycleArithmetic} with the given index count and spacing.
     *
     * @param indexCount   The number of index entries allowed
     * @param indexSpacing The spacing between indexed entries
     * @return A new instance of {@link RollCycleArithmetic}
     */
    public static RollCycleArithmetic of(int indexCount, int indexSpacing) {
        return new RollCycleArithmetic(indexCount, indexSpacing);
    }

    /**
     * Private constructor to initialize a {@link RollCycleArithmetic} instance.
     *
     * @param indexCount   The number of index entries (power of 2)
     * @param indexSpacing The spacing between indexed entries (power of 2)
     */
    private RollCycleArithmetic(int indexCount, int indexSpacing) {
        this.indexCount = Maths.nextPower2(indexCount, 8);
        assert this.indexCount <= MAX_INDEX_COUNT : "indexCount: " + indexCount;
        this.indexSpacing = Maths.nextPower2(indexSpacing, 1);
        cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));
        assert cycleShift < Long.SIZE : "cycleShift: " + cycleShift;
        sequenceMask = (1L << cycleShift) - 1;
    }

    /**
     * Returns the maximum number of messages that can be stored in a single cycle.
     *
     * @return The maximum number of messages per cycle
     */
    public long maxMessagesPerCycle() {
        return Math.min(sequenceMask, ((long) indexCount * indexCount * indexSpacing));
    }

    /**
     * Converts a cycle and sequence number to a unique index.
     *
     * @param cycle         The cycle number
     * @param sequenceNumber The sequence number within the cycle
     * @return The calculated index
     */
    public long toIndex(int cycle, long sequenceNumber) {
        return ((long) cycle << cycleShift) + (sequenceNumber & sequenceMask);
    }

    /**
     * Extracts the sequence number from a given index.
     *
     * @param index The index to extract from
     * @return The sequence number
     */
    public long toSequenceNumber(long index) {
        return index & sequenceMask;
    }

    /**
     * Extracts the cycle number from a given index.
     *
     * @param index The index to extract from
     * @return The cycle number
     */
    public int toCycle(long index) {
        return Maths.toUInt31(index >> cycleShift);
    }

    /**
     * Returns the configured index spacing.
     *
     * @return The index spacing
     */
    public int indexSpacing() {
        return indexSpacing;
    }

    /**
     * Returns the configured index count.
     *
     * @return The index count
     */
    public int indexCount() {
        return indexCount;
    }
}
