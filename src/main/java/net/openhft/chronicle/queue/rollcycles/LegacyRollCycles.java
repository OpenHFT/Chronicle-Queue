/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
