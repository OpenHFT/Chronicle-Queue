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
 * Enum representing large roll cycles, designed to minimize file rolls but resulting in very large files.
 * <p>These roll cycles are typically used in scenarios where fewer rollovers are preferred, but the file sizes
 * can grow quite large and may exceed typical limits.</p>
 */
public enum LargeRollCycles implements RollCycle {

    /**
     * Roll cycle allowing up to 0xffffffff entries per hour, indexing every 64th entry.
     */
    LARGE_HOURLY(/*----*/"yyyyMMdd-HH'L'", 60 * 60 * 1000, 8 << 10, 64),
    /**
     * Roll cycle allowing up to 0x1fffffffff entries per day, indexing every 128th entry.
     */
    LARGE_DAILY(/*-----*/"yyyyMMdd'L'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 128),
    /**
     * Roll cycle allowing up to 0x3ffffffffff entries per day, indexing every 256th entry.
     */
    XLARGE_DAILY(/*----*/"yyyyMMdd'X'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 256),
    /**
     * Roll cycle allowing up to 0xffffffffffff entries per day, with sparse indexing (every 1024th entry).
     */
    HUGE_DAILY(/*------*/"yyyyMMdd'H'", 24 * 60 * 60 * 1000, MAX_INDEX_COUNT, 1024),
    ;

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;

    /**
     * Constructs a LargeRollCycle with the given parameters.
     *
     * @param format          The format string used for rolling files
     * @param lengthInMillis  The duration of each cycle in milliseconds
     * @param indexCount      The number of index entries
     * @param indexSpacing    The spacing between indexed entries
     */
    LargeRollCycles(String format, int lengthInMillis, int indexCount, int indexSpacing) {
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
