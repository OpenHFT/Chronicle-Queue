/*
 * Copyright 2022 Higher Frequency Trading
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

package net.openhft.chronicle.queue.rollcycles;

import net.openhft.chronicle.queue.RollCycle;

/**
 * Enum representing various test roll cycles, designed to reduce the size of a queue dump
 * when performing small tests.
 * <p>These roll cycles are intended for testing purposes only and are not suited for production use
 * due to their limited capacity and reduced indexing granularity.</p>
 */
public enum TestRollCycles implements RollCycle {

    /**
     * Roll cycle allowing up to 0xffffffff entries per second. Only suitable for testing.
     */
    TEST_SECONDLY(/*---*/"yyyyMMdd-HHmmss'T'", 1000, MAX_INDEX_COUNT, 4),

    /**
     * Roll cycle allowing up to 0x1000 entries per second. Only suitable for testing.
     */
    TEST4_SECONDLY(/*---*/"yyyyMMdd-HHmmss'T4'", 1000, 32, 4),

    /**
     * Roll cycle allowing up to 0x400 entries per hour. Only suitable for testing.
     */
    TEST_HOURLY(/*-----*/"yyyyMMdd-HH'T'", 60 * 60 * 1000, 16, 4),

    /**
     * Roll cycle allowing up to 0x40 entries per day. Only suitable for testing.
     */
    TEST_DAILY(/*------*/"yyyyMMdd'T1'", 24 * 60 * 60 * 1000, 8, 1),

    /**
     * Roll cycle allowing up to 0x200 entries per day. Only suitable for testing.
     */
    TEST2_DAILY(/*-----*/"yyyyMMdd'T2'", 24 * 60 * 60 * 1000, 16, 2),

    /**
     * Roll cycle allowing up to 0x1000 entries per day. Only suitable for testing.
     */
    TEST4_DAILY(/*-----*/"yyyyMMdd'T4'", 24 * 60 * 60 * 1000, 32, 4),

    /**
     * Roll cycle allowing up to 0x20000 entries per day. Only suitable for testing.
     */
    TEST8_DAILY(/*-----*/"yyyyMMdd'T8'", 24 * 60 * 60 * 1000, 128, 8),
    ;

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;

    /**
     * Constructs a TestRollCycle with the given parameters.
     *
     * @param format          The format string used for rolling files
     * @param lengthInMillis  The duration of each cycle in milliseconds
     * @param indexCount      The number of index entries
     * @param indexSpacing    The spacing between indexed entries
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
     * <p>Note: {@code indexCount^2} is the maximum number of index queue entries.</p>
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
