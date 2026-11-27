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
     * 0x1000000 entries - Only good for testing
     */
    //! A 32,768-entry index forces a 1 MiB block even when a test requests
    //! small mappings. With 2,048 entries, the index permits the 64 KiB
    //! Windows block minimum, reducing the initial small-block roll-file
    //! extent from 1.25 MiB to 128 KiB. Spacing 4 still supports 16,777,216
    //! messages in each one-second cycle: FIX/Queue does not write at that
    //! rate in these test workloads. Tests that hold their clock fixed must
    //! still respect this per-cycle capacity; the roll period is unchanged.
    TEST_SECONDLY(/*---*/"yyyyMMdd-HHmmss'T'", 1000, 2_048, 4),
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
