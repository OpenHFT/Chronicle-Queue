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
 * Enum representing legacy roll cycles, kept for historical reasons.
 * <p>These roll cycles were used in older versions of Chronicle Queue and retain their original
 * configurations for backward compatibility.</p>
 */
public enum LegacyRollCycles implements RollCycle {

    /**
     * Roll cycle allowing up to 0x4000000 entries per minute, indexing every 16th entry.
     */
    MINUTELY(/*--------*/"yyyyMMdd-HHmm", 60 * 1000, 2 << 10, 16),
    /**
     * Roll cycle allowing up to 0x10000000 entries per hour, indexing every 16th entry.
     * <p>Maintained as 4K index count and 16 index spacing for historical reasons.</p>
     */
    HOURLY(/*----------*/"yyyyMMdd-HH", 60 * 60 * 1000, 4 << 10, 16),
    /**
     * Roll cycle allowing up to 0xffffffff entries per day, indexing every 64th entry.
     * <p>Maintained as 8K index count and 64 index spacing for historical reasons.</p>
     */
    DAILY(/*-----------*/"yyyyMMdd", 24 * 60 * 60 * 1000, 8 << 10, 64),
    ;

    private final String format;
    private final int lengthInMillis;
    private final RollCycleArithmetic arithmetic;

    /**
     * Constructs a LegacyRollCycle with the given parameters.
     *
     * @param format          The format string used for rolling files
     * @param lengthInMillis  The duration of each cycle in milliseconds
     * @param indexCount      The number of index entries
     * @param indexSpacing    The spacing between indexed entries
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
