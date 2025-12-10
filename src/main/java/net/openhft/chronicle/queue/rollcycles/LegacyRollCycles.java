/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.rollcycles;

import net.openhft.chronicle.queue.RollCycle;

/**
 * Enum representing legacy roll cycles, kept for historical reasons.
 * <p>These roll cycles were used in older versions of Chronicle Queue and retain their original
 * configurations for backward compatibility.
 */
public enum LegacyRollCycles implements RollCycle {
    /**
     * 0x4000000 entries per minute, indexing every 16th entry
     */
    MINUTELY(/*--------*/"yyyyMMdd-HHmm", 60 * 1000, 2 << 10, 16),
    /**
     * 0x10000000 entries per hour, indexing every 16th entry, leave as 4K and 16 for historical reasons.
     */
    HOURLY(/*----------*/"yyyyMMdd-HH", 60 * 60 * 1000, 4 << 10, 16),
    /**
     * 0xffffffff entries per day, indexing every 64th entry, leave as 8K and 64 for historical reasons.
     */
    DAILY(/*-----------*/"yyyyMMdd", 24 * 60 * 60 * 1000, 8 << 10, 64);

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;

    /**
     * Constructs a LegacyRollCycle with the given parameters.
     *
     * @param format         The format string used for rolling files
     * @param lengthInMillis The duration of each cycle in milliseconds
     * @param indexCount     The number of index entries
     * @param indexSpacing   The spacing between indexed entries
     */
    LegacyRollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing) {
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
