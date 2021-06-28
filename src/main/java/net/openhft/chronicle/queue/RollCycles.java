/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.time.TimeProvider;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

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

    // these are kept for historical reasons
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
    DAILY(/*-----------*/"yyyyMMdd", 24 * 60 * 60 * 1000, 8 << 10, 64),

    // these are used to minimise rolls but do create very large files, possibly too large.
    /**
     * 0xffffffff entries per hour, indexing every 64th entry
     */
    LARGE_HOURLY(/*----*/"yyyyMMdd-HH'L'", 60 * 60 * 1000, 8 << 10, 64),
    /**
     * 0x1fffffffff entries per day, indexing every 128th entry
     */
    LARGE_DAILY(/*-----*/"yyyyMMdd'L'", 24 * 60 * 60 * 1000, 32 << 10, 128),
    /**
     * 0x3ffffffffff entries per day, indexing every 256th entry
     */
    XLARGE_DAILY(/*----*/"yyyyMMdd'X'", 24 * 60 * 60 * 1000, 128 << 10, 256),
    /**
     * 0xffffffffffff entries per day with sparse indexing (every 1024th entry)
     */
    HUGE_DAILY(/*------*/"yyyyMMdd'H'", 24 * 60 * 60 * 1000, 512 << 10, 1024),

    // these are largely used for testing and benchmarks to almost turn off indexing.
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
    HUGE_DAILY_XSPARSE("yyyyMMdd'HX'", 24 * 60 * 60 * 1000, 16 << 10, 1 << 20),

    // these are used for test to reduce the size of a queue dump when doing a small test.
    /**
     * 0xffffffff entries - Only good for testing
     */
    TEST_SECONDLY(/*---*/"yyyyMMdd-HHmmss'T'", 1000, 1 << 15, 4),
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
    TEST8_DAILY(/*-----*/"yyyyMMdd'T8'", 24 * 60 * 60 * 1000, 128, 8),;
    public static final RollCycles DEFAULT = FAST_DAILY;

    // don't alter this or you will confuse yourself.
    private static final Iterable<RollCycles> VALUES = Arrays.asList(values());

    private final String format;
    private final int lengthInMillis;
    private final int cycleShift;
    private final int indexCount;
    private final int indexSpacing;
    private final long sequenceMask;

    RollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing) {
        this.format = format;
        this.lengthInMillis = lengthInMillis;
        this.indexCount = Maths.nextPower2(indexCount, 8);
        this.indexSpacing = Maths.nextPower2(indexSpacing, 1);
        cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));
        sequenceMask = (1L << cycleShift) - 1;
    }

    public long maxMessagesPerCycle() {
        return maxMessagesPerCycle(indexCount, indexSpacing);
    }

    public static Iterable<RollCycles> all() {
        return VALUES;
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
     * @return this is the size of each index array, note: indexCount^2 is the maximum number of index queue entries.
     */
    @Override
    public int defaultIndexCount() {
        return indexCount;
    }

    @Override
    public int defaultIndexSpacing() {
        return indexSpacing;
    }

    @Override
    public int current(@NotNull TimeProvider time, long epoch) {
        return (int) ((time.currentTimeMillis() - epoch) / lengthInMillis());
    }

    @Override
    public long toIndex(int cycle, long sequenceNumber) {
        return ((long) cycle << cycleShift) + (sequenceNumber & sequenceMask);
    }

    @Override
    public long toSequenceNumber(long index) {
        return index & sequenceMask;
    }

    @Override
    public int toCycle(long index) {
        return Maths.toUInt31(index >> cycleShift);
    }

    public static long maxMessagesPerCycle(final long indexCount0, final int indexSpacing0) {

        // these are inline with the SQ Indexing code
        long indexCount = Maths.nextPower2(indexCount0, 8);

        // these are inline with the SQ Indexing code
        int indexSpacing = Maths.nextPower2(indexSpacing0, 1);

        int cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));
        long sequenceMask0 = (1L << cycleShift) - 1L;

        return Math.min(sequenceMask0, indexCount * indexCount * indexSpacing);
    }
}
