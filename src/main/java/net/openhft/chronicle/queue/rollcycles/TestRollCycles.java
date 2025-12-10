/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.rollcycles;

import net.openhft.chronicle.queue.RollCycle;

/**
 * Enum representing various test roll cycles, designed to reduce the size of a queue dump
 * when performing small tests.
 * <p>These roll cycles are intended for testing purposes only and are not suited for production use
 * due to their limited capacity and reduced indexing granularity.
 */
public enum TestRollCycles implements RollCycle {
    /**
     * 0xffffffff entries - Only good for testing
     */
    TEST_SECONDLY(/*---*/"yyyyMMdd-HHmmss'T'", 1000, MAX_INDEX_COUNT, 4),
    /**
     * 0x1000 entries - Only good for testing
     */
    TEST4_SECONDLY(/*---*/"yyyyMMdd-HHmmss'T4'", 1000, 32, 4),
    /**
     * 0x400 entries per hour - Only good for testing
     */
    TEST_HOURLY(/*-----*/"yyyyMMdd-HH'T'", 60 * 60 * 1000, 16, 4),
    /**
     * 0x40 entries per day - Only good for testing
     */
    TEST_DAILY(/*------*/"yyyyMMdd'T1'", 24 * 60 * 60 * 1000, 8, 1),
    /**
     * 0x200 entries per day - Only good for testing
     */
    TEST2_DAILY(/*-----*/"yyyyMMdd'T2'", 24 * 60 * 60 * 1000, 16, 2),
    /**
     * 0x1000 entries per day - Only good for testing
     */
    TEST4_DAILY(/*-----*/"yyyyMMdd'T4'", 24 * 60 * 60 * 1000, 32, 4),
    /**
     * 0x20000 entries per day - Only good for testing
     */
    TEST8_DAILY(/*-----*/"yyyyMMdd'T8'", 24 * 60 * 60 * 1000, 128, 8);

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;

    /**
     * Constructs a TestRollCycle with the given parameters.
     *
     * @param format         The format string used for rolling files
     * @param lengthInMillis The duration of each cycle in milliseconds
     * @param indexCount     The number of index entries
     * @param indexSpacing   The spacing between indexed entries
     */
    TestRollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing) {
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
