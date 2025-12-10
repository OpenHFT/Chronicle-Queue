/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.rollcycles;

import net.openhft.chronicle.queue.RollCycle;

/**
 * Enum representing large roll cycles, designed to minimize file rolls but resulting in very large files.
 * <p>These roll cycles are typically used in scenarios where fewer rollovers are preferred, but the file sizes
 * can grow quite large and may exceed typical limits.
 */
public enum LargeRollCycles implements RollCycle {
    /**
     * 0xffffffff entries per hour, indexing every 64th entry
     */
    LARGE_HOURLY(/*----*/"yyyyMMdd-HH'L'", 60 * 60 * 1000, 8 << 10, 64),
    /**
     * 0x1fffffffff entries per day, indexing every 128th entry
     */
    LARGE_DAILY(/*-----*/"yyyyMMdd'L'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 128),
    /**
     * 0x3ffffffffff entries per day, indexing every 256th entry
     */
    XLARGE_DAILY(/*----*/"yyyyMMdd'X'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 256),
    /**
     * 0xffffffffffff entries per day with sparse indexing (every 1024th entry)
     */
    HUGE_DAILY(/*------*/"yyyyMMdd'H'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 1024);

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;

    /**
     * Constructs a LargeRollCycle with the given parameters.
     *
     * @param format         The format string used for rolling files
     * @param lengthInMillis The duration of each cycle in milliseconds
     * @param indexCount     The number of index entries
     * @param indexSpacing   The spacing between indexed entries
     */
    LargeRollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing) {
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
}
