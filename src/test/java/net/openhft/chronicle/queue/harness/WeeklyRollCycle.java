/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.harness;

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.RollCycleArithmetic;

import java.util.concurrent.TimeUnit;

public class WeeklyRollCycle implements RollCycle {

    public static final WeeklyRollCycle INSTANCE = new WeeklyRollCycle("yyyyDDD", (int) TimeUnit.DAYS.toMillis(7L), 16384, 16);

    private final String format;
    private final int length;
    private final RollCycleArithmetic arithmetic;

    private WeeklyRollCycle(String format, int length, int indexCount, int indexSpacing) {
        this.format = format;
        this.length = length;
        arithmetic = RollCycleArithmetic.of(indexCount, indexSpacing);
    }

    @Override
    public String format() {
        return this.format;
    }

    @Override
    public int lengthInMillis() {
        return this.length;
    }

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

    @Override
    public long maxMessagesPerCycle() {
        return arithmetic.maxMessagesPerCycle();
    }
}
