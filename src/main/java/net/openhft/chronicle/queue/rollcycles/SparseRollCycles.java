/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.rollcycles;

import net.openhft.chronicle.queue.RollCycle;

/**
 * Enum representing sparse roll cycles, primarily used for testing and benchmarking purposes.
 * <p>These roll cycles are designed to minimize indexing, making them useful for scenarios where
 * indexing is either unnecessary or should be kept minimal to reduce overhead.
 */
public enum SparseRollCycles implements RollCycle {
    /**
     * 0x20000000 entries per day, indexing every 8th entry
     */
    SMALL_DAILY(/*-----*/"yyyyMMdd'S'", 24 * 60 * 60 * 1000, 8 << 10, 8),
    /**
     * 0x3ffffffff entries per hour with sparse indexing (every 1024th entry)
     */
    LARGE_HOURLY_SPARSE("yyyyMMdd-HH'LS'", 60 * 60 * 1000, 4 << 10, 1024),
    /**
     * 0x3ffffffffff entries per hour with super-sparse indexing (every (2^20)th entry)
     */
    LARGE_HOURLY_XSPARSE("yyyyMMdd-HH'LX'", 60 * 60 * 1000, 2 << 10, 1 << 20),
    /**
     * 0xffffffffffff entries per day with super-sparse indexing (every (2^20)th entry)
     */
    HUGE_DAILY_XSPARSE("yyyyMMdd'HX'", 24 * 60 * 60 * 1000, 16 << 10, 1 << 20);

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;

    /**
     * Constructs a SparseRollCycle with the given parameters.
     *
     * @param format         The format string used for rolling files
     * @param lengthInMillis The duration of each cycle in milliseconds
     * @param indexCount     The number of index entries
     * @param indexSpacing   The spacing between indexed entries
     */
    SparseRollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing) {
        this.format = format;
        this.lengthInMillis = lengthInMillis;
        this.arithmetic = RollCycleArithmetic.of(indexCount, indexSpacing);
    }

    /**
     * Returns the maximum number of messages allowed per cycle.
     *
     * @return The maximum number of messages allowed per cycle
     */
    public long maxMessagesPerCycle() {
        return arithmetic.maxMessagesPerCycle();
    }

    @Override
    public String format() {
        return this.format;
    }

    @Override
    public int lengthInMillis() {
        return this.lengthInMillis;
    }

    /**
     * Returns the default size of the index array.
     * <p>Note: {@code indexCount^2} is the maximum number of index queue entries.
     *
     * @return The default index count
     */
    @Override
    public int defaultIndexCount() {
        return arithmetic.indexCount();
    }

    @Override
    public int defaultIndexSpacing() {
        return arithmetic.indexSpacing();
    }

    @Override
    public long toIndex(int cycle, long sequenceNumber) {
        return arithmetic.toIndex(cycle, sequenceNumber);
    }

    @Override
    public long toSequenceNumber(long index) {
        return arithmetic.toSequenceNumber(index);
    }

    @Override
    public int toCycle(long index) {
        return arithmetic.toCycle(index);
    }
}
