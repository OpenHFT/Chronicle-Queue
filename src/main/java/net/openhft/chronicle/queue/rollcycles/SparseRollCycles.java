/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

    @Override
    public String format() {
        return this.format;
    }

    @Override
    public int lengthInMillis() {
        return this.lengthInMillis;
    }

    @Override
    public RollCycleArithmetic arithmetic() {
        return this.arithmetic;
    }

    // Backward compatibility: retain methods that used to be declared on this enum
    // CPD-OFF
    @Override
    public int defaultIndexCount() {
        return RollCycle.super.defaultIndexCount();
    }

    @Override
    public int defaultIndexSpacing() {
        return RollCycle.super.defaultIndexSpacing();
    }

    @Override
    public long maxMessagesPerCycle() {
        return RollCycle.super.maxMessagesPerCycle();
    }

    @Override
    public int toCycle(long index) {
        return RollCycle.super.toCycle(index);
    }

    @Override
    public long toIndex(int cycle, long sequenceNumber) {
        return RollCycle.super.toIndex(cycle, sequenceNumber);
    }

    @Override
    public long toSequenceNumber(long index) {
        return RollCycle.super.toSequenceNumber(index);
    }
    // CPD-ON
}
