/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.queue.rollcycles.*;

import java.util.*;
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
    WEEKLY(/*-----------*/"YYYY'W'ww", 7 * 24 * 60 * 60 * 1000, 4 << 10, 256, RollCycleArithmetic.SUNDAY_00_00),

    // these are kept for historical reasons
    /**
     * 0x4000000 entries per minute, indexing every 16th entry
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.LegacyRollCycles#MINUTELY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    MINUTELY(/*--------*/"yyyyMMdd-HHmm", 60 * 1000, 2 << 10, 16),
    /**
     * 0x10000000 entries per hour, indexing every 16th entry, leave as 4K and 16 for historical reasons.
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.LegacyRollCycles#HOURLY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    HOURLY(/*----------*/"yyyyMMdd-HH", 60 * 60 * 1000, 4 << 10, 16),
    /**
     * 0xffffffff entries per day, indexing every 64th entry, leave as 8K and 64 for historical reasons.
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.LegacyRollCycles#DAILY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    DAILY(/*-----------*/"yyyyMMdd", 24 * 60 * 60 * 1000, 8 << 10, 64),

    // these are used to minimise rolls but do create very large files, possibly too large.
    /**
     * 0xffffffff entries per hour, indexing every 64th entry
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.LargeRollCycles#LARGE_HOURLY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    LARGE_HOURLY(/*----*/"yyyyMMdd-HH'L'", 60 * 60 * 1000, 8 << 10, 64),
    /**
     * 0x1fffffffff entries per day, indexing every 128th entry
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.LargeRollCycles#LARGE_DAILY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    LARGE_DAILY(/*-----*/"yyyyMMdd'L'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 128),
    /**
     * 0x3ffffffffff entries per day, indexing every 256th entry
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.LargeRollCycles#XLARGE_DAILY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    XLARGE_DAILY(/*----*/"yyyyMMdd'X'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 256),
    /**
     * 0xffffffffffff entries per day with sparse indexing (every 1024th entry)
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.LargeRollCycles#HUGE_DAILY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    HUGE_DAILY(/*------*/"yyyyMMdd'H'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 1024),

    // these are largely used for testing and benchmarks to almost turn off indexing.
    /**
     * 0x20000000 entries per day, indexing every 8th entry
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.SparseRollCycles#SMALL_DAILY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    SMALL_DAILY(/*-----*/"yyyyMMdd'S'", 24 * 60 * 60 * 1000, 8 << 10, 8),
    /**
     * 0x3ffffffff entries per hour with sparse indexing (every 1024th entry)
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.SparseRollCycles#LARGE_HOURLY_SPARSE} instead
     */
    @Deprecated(/* For removal in x.26 */)
    LARGE_HOURLY_SPARSE("yyyyMMdd-HH'LS'", 60 * 60 * 1000, 4 << 10, 1024),
    /**
     * 0x3ffffffffff entries per hour with super-sparse indexing (every (2^20)th entry)
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.SparseRollCycles#LARGE_HOURLY_XSPARSE} instead
     */
    @Deprecated(/* For removal in x.26 */)
    LARGE_HOURLY_XSPARSE("yyyyMMdd-HH'LX'", 60 * 60 * 1000, 2 << 10, 1 << 20),
    /**
     * 0xffffffffffff entries per day with super-sparse indexing (every (2^20)th entry)
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.SparseRollCycles#HUGE_DAILY_XSPARSE} instead
     */
    @Deprecated(/* For removal in x.26 */)
    HUGE_DAILY_XSPARSE("yyyyMMdd'HX'", 24 * 60 * 60 * 1000, 16 << 10, 1 << 20),

    // these are used for test to reduce the size of a queue dump when doing a small test.
    /**
     * 0xffffffff entries - Only good for testing
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.TestRollCycles#TEST_SECONDLY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    TEST_SECONDLY(/*---*/"yyyyMMdd-HHmmss'T'", 1000, MAX_INDEX_COUNT, 4),
    /**
     * 0x1000 entries - Only good for testing
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.TestRollCycles#TEST4_SECONDLY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    TEST4_SECONDLY(/*---*/"yyyyMMdd-HHmmss'T4'", 1000, 32, 4),
    /**
     * 0x400 entries per hour - Only good for testing
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.TestRollCycles#TEST_HOURLY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    TEST_HOURLY(/*-----*/"yyyyMMdd-HH'T'", 60 * 60 * 1000, 16, 4),
    /**
     * 0x40 entries per day - Only good for testing
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.TestRollCycles#TEST_DAILY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    TEST_DAILY(/*------*/"yyyyMMdd'T1'", 24 * 60 * 60 * 1000, 8, 1),
    /**
     * 0x200 entries per day - Only good for testing
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.TestRollCycles#TEST2_DAILY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    TEST2_DAILY(/*-----*/"yyyyMMdd'T2'", 24 * 60 * 60 * 1000, 16, 2),
    /**
     * 0x1000 entries per day - Only good for testing
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.TestRollCycles#TEST4_DAILY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    TEST4_DAILY(/*-----*/"yyyyMMdd'T4'", 24 * 60 * 60 * 1000, 32, 4),
    /**
     * 0x20000 entries per day - Only good for testing
     *
     * @deprecated Use {@link net.openhft.chronicle.queue.rollcycles.TestRollCycles#TEST8_DAILY} instead
     */
    @Deprecated(/* For removal in x.26 */)
    TEST8_DAILY(/*-----*/"yyyyMMdd'T8'", 24 * 60 * 60 * 1000, 128, 8),
    ;
    public static final RollCycles DEFAULT = FAST_DAILY;
    private static final Map<RollCycle, Enum<?>> DEPRECATED_MAPPINGS;

    // don't alter this or you will confuse yourself.
    private static final Iterable<RollCycle> VALUES;

    static {
        DEPRECATED_MAPPINGS = initialiseDeprecatedMappings();
        List<RollCycle> allCycles = new ArrayList<>();
        // We can add #values() again once the deprecated ones are gone
        allCycles.addAll(Arrays.stream(RollCycles.values()).filter(rc -> !isDeprecated(rc)).collect(Collectors.toList()));
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

    RollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing) {
        this(format, lengthInMillis, indexCount, indexSpacing, 0);
    }
    RollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing, int defaultEpoch) {
        this.format = format;
        this.lengthInMillis = lengthInMillis;
        this.defaultEpoch = defaultEpoch;
        this.arithmetic = RollCycleArithmetic.of(indexCount, indexSpacing);
    }

    public static Iterable<RollCycle> all() {
        return VALUES;
    }

    /**
     * @deprecated use {@link RollCycleArithmetic#maxMessagesPerCycle()} instead
     */
    @Deprecated(/* For removal in x.26 */)
    public static long maxMessagesPerCycle(final long indexCount0, final int indexSpacing0) {

        // these are inline with the SQ Indexing code
        long indexCount = Maths.nextPower2(indexCount0, 8);

        // these are inline with the SQ Indexing code
        int indexSpacing = Maths.nextPower2(indexSpacing0, 1);

        int cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));
        long sequenceMask0 = (1L << cycleShift) - 1L;

        return Math.min(sequenceMask0, indexCount * indexCount * indexSpacing);
    }

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

    /**
     * Check if the provided roll cycle is deprecated, log a warning if it is
     * <p>
     * NOTE: This will be removed when the deprecated RollCycles are removed in x.26
     *
     * @param rollCycle The RollCycle to check
     * @return The RollCycle passed in
     */
    public static RollCycle warnIfDeprecated(RollCycle rollCycle) {
        final Enum<?> replacementEnum = DEPRECATED_MAPPINGS.get(rollCycle);
        if (replacementEnum != null) {
            Jvm.warn().on(RollCycles.class,
                    "You've configured your queue to use a deprecated RollCycle ("
                            + renderEnum((Enum<?>) rollCycle)
                            + ") please consider switching to " +
                            renderEnum(replacementEnum)
                            + ", as the RollCycle constant you've nominated will be removed in a future release!");
        }
        return rollCycle;
    }

    /**
     * Is a particular roll cycle deprecated
     * <p>
     * NOTE: This will be removed when the deprecated RollCycles are removed in x.26
     *
     * @param rollCycle The roll cycle to check
     * @return true if it's deprecated, false otherwise
     */
    static boolean isDeprecated(RollCycle rollCycle) {
        return DEPRECATED_MAPPINGS.containsKey(rollCycle);
    }

    private static String renderEnum(Enum<?> enumVal) {
        return enumVal.getClass().getName() + "." + enumVal.name();
    }

    private static Map<RollCycle, Enum<?>> initialiseDeprecatedMappings() {
        Map<RollCycle, Enum<?>> mappings = new HashMap<>();

        // Test RCs
        mappings.put(RollCycles.TEST_SECONDLY, TestRollCycles.TEST_SECONDLY);
        mappings.put(RollCycles.TEST4_SECONDLY, TestRollCycles.TEST4_SECONDLY);
        mappings.put(RollCycles.TEST_HOURLY, TestRollCycles.TEST_HOURLY);
        mappings.put(RollCycles.TEST_DAILY, TestRollCycles.TEST_DAILY);
        mappings.put(RollCycles.TEST2_DAILY, TestRollCycles.TEST2_DAILY);
        mappings.put(RollCycles.TEST4_DAILY, TestRollCycles.TEST4_DAILY);
        mappings.put(RollCycles.TEST8_DAILY, TestRollCycles.TEST8_DAILY);

        // Sparse RCs
        mappings.put(RollCycles.SMALL_DAILY, SparseRollCycles.SMALL_DAILY);
        mappings.put(RollCycles.LARGE_HOURLY_SPARSE, SparseRollCycles.LARGE_HOURLY_SPARSE);
        mappings.put(RollCycles.LARGE_HOURLY_XSPARSE, SparseRollCycles.LARGE_HOURLY_XSPARSE);
        mappings.put(RollCycles.HUGE_DAILY_XSPARSE, SparseRollCycles.HUGE_DAILY_XSPARSE);

        // Legacy RCs
        mappings.put(RollCycles.MINUTELY, LegacyRollCycles.MINUTELY);
        mappings.put(RollCycles.HOURLY, LegacyRollCycles.HOURLY);
        mappings.put(RollCycles.DAILY, LegacyRollCycles.DAILY);

        // Large RCs
        mappings.put(RollCycles.LARGE_HOURLY, LargeRollCycles.LARGE_HOURLY);
        mappings.put(RollCycles.LARGE_DAILY, LargeRollCycles.LARGE_DAILY);
        mappings.put(RollCycles.XLARGE_DAILY, LargeRollCycles.XLARGE_DAILY);
        mappings.put(RollCycles.HUGE_DAILY, LargeRollCycles.HUGE_DAILY);

        return Collections.unmodifiableMap(mappings);
    }
}
