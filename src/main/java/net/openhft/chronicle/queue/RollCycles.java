/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.rollcycles.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Roll cycles to use with the queue. Sparse indexing roll cycles are useful for improving write performance but they slightly slow random access
 * performance (with no effect for sequential reads)
 */
public enum RollCycles implements RollCycle {
    /**
     * 0x40000000 entries per 5 minutes, indexing every 256th entry
     */
    FIVE_MINUTELY(/*------*/"yyyyMMdd-HHmm'V'", 5 * 60 * 1000, 2 << 10, 256),
    /**
     * 0x40000000 entries per 10 minutes, indexing every 256th entry
     */
    TEN_MINUTELY(/*------*/"yyyyMMdd-HHmm'X'", 10 * 60 * 1000, 2 << 10, 256),
    /**
     * 0x40000000 entries per 20 minutes, indexing every 256th entry
     */
    TWENTY_MINUTELY(/*------*/"yyyyMMdd-HHmm'XX'", 20 * 60 * 1000, 2 << 10, 256),
    /**
     * 0x40000000 entries per half hour, indexing every 256th entry
     */
    HALF_HOURLY(/*------*/"yyyyMMdd-HHmm'H'", 30 * 60 * 1000, 2 << 10, 256),
    /**
     * 0xffffffff entries per hour, indexing every 256th entry, leave as 4K and 256 for historical reasons.
     */
    FAST_HOURLY(/*----------*/"yyyyMMdd-HH'F'", 60 * 60 * 1000, 4 << 10, 256),
    /**
     * 0xffffffff entries per 2 hours, indexing every 256th entry
     */
    TWO_HOURLY(/*----*/"yyyyMMdd-HH'II'", 2 * 60 * 60 * 1000, 4 << 10, 256),
    /**
     * 0xffffffff entries per four hours, indexing every 256th entry
     */
    FOUR_HOURLY(/*----*/"yyyyMMdd-HH'IV'", 4 * 60 * 60 * 1000, 4 << 10, 256),
    /**
     * 0xffffffff entries per six hours, indexing every 256th entry
     */
    SIX_HOURLY(/*----*/"yyyyMMdd-HH'VI'", 6 * 60 * 60 * 1000, 4 << 10, 256),
    /**
     * 0xffffffff entries per day, indexing every 256th entry, leave as 4K and 256 for historical reasons.
     */
    FAST_DAILY(/*-----------*/"yyyyMMdd'F'", 24 * 60 * 60 * 1000, 4 << 10, 256),
    /**
     * 0xffffffff entries per week, indexing every 256th entry, leave as 4K and 256 for historical reasons. Cycle starts Sunday 00:00
     */
    WEEKLY(/*-----------*/"YYYY'W'ww", 7 * 24 * 60 * 60 * 1000, 4 << 10, 256, RollCycleArithmetic.SUNDAY_00_00);
    public static final RollCycles DEFAULT = FAST_DAILY;

    // don't alter this or you will confuse yourself.
    private static final Iterable<RollCycle> VALUES;

    static {
        List<RollCycle> allCycles = new ArrayList<>();
        // We can add #values() again once the deprecated ones are gone
        allCycles.addAll(Arrays.stream(RollCycles.values()).collect(Collectors.toList()));
        allCycles.addAll(Arrays.asList(LegacyRollCycles.values()));
        allCycles.addAll(Arrays.asList(LargeRollCycles.values()));
        allCycles.addAll(Arrays.asList(SparseRollCycles.values()));
        allCycles.addAll(Arrays.asList(TestRollCycles.values()));
        VALUES = Collections.unmodifiableList(allCycles);
    }

    private final String format;
    private final int lengthInMillis;
    private final int defaultEpoch;
    private final RollCycleArithmetic arithmetic;
    private final long maxMessagesPerCycle;

    RollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing) {
        this(format, lengthInMillis, indexCount, indexSpacing, 0);
    }

    RollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing, int defaultEpoch) {
        this.format = format;
        this.lengthInMillis = lengthInMillis;
        this.defaultEpoch = defaultEpoch;
        this.arithmetic = RollCycleArithmetic.of(indexCount, indexSpacing);
        maxMessagesPerCycle = arithmetic.maxMessagesPerCycle();
    }

    public static Iterable<RollCycle> all() {
        return VALUES;
    }

    public long maxMessagesPerCycle() {
        return maxMessagesPerCycle;
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
    public int defaultEpoch() {
        return this.defaultEpoch;
    }

    /**
     * @return this is the size of each index array, note: indexCount^2 is the maximum number of index queue entries.
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
