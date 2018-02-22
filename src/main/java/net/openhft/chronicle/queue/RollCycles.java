/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import org.jetbrains.annotations.NotNull;

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.time.TimeProvider;

public enum RollCycles implements RollCycle {
    TEST_SECONDLY(/*--*/"yyyyMMdd-HHmmss", 1000, 1 << 15, 4), // only good for testing
    MILLI_WEEKLY(/*---*/"yyyyMMdd-HHmmssSSS", 7 * 24 * 60 * 60 * 1000, 4 << 10, 128), // 4 trillion entries per milli-week
    MINUTELY(/*-------*/"yyyyMMdd-HHmm", 60 * 1000, 2 << 10, 16), // 64 million entries per minute
    TEST_HOURLY(/*----*/"yyyyMMdd-HH", 60 * 60 * 1000, 16, 4), // 512 entries per hour.
    HOURLY(/*---------*/"yyyyMMdd-HH", 60 * 60 * 1000, 4 << 10, 16), // 256 million entries per hour.
    LARGE_HOURLY(/*---*/"yyyyMMdd-HH", 60 * 60 * 1000, 8 << 10, 64), // 2 billion entries per hour.
    LARGE_HOURLY_SPARSE("yyyyMMdd-HH", 60 * 60 * 1000, 4 << 10, 1024), // 16 billion entries per hour with sparse indexing
    TEST_DAILY(/*-----*/"yyyyMMdd", 24 * 60 * 60 * 1000, 8, 1), // Only good for testing - 63 entries per day
    TEST2_DAILY(/*----*/"yyyyMMdd", 24 * 60 * 60 * 1000, 16, 2), // Only good for testing
    TEST4_DAILY(/*----*/"yyyyMMdd", 24 * 60 * 60 * 1000, 32, 4), // Only good for testing
    SMALL_DAILY(/*----*/"yyyyMMdd", 24 * 60 * 60 * 1000, 8 << 10, 8), // 512 million entries per day
    DAILY(/*----------*/"yyyyMMdd", 24 * 60 * 60 * 1000, 8 << 10, 64), // 4 billion entries per day
    LARGE_DAILY(/*----*/"yyyyMMdd", 24 * 60 * 60 * 1000, 32 << 10, 128), // 128 billion entries per day
    XLARGE_DAILY(/*---*/"yyyyMMdd", 24 * 60 * 60 * 1000, 128 << 10, 256), // 4 trillion entries per day
    HUGE_DAILY(/*-----*/"yyyyMMdd", 24 * 60 * 60 * 1000, 512 << 10, 1024), // 256 trillion entries per day
    ;

    final String format;
    final int length;
    final int cycleShift;
    final int indexCount;
    final int indexSpacing;
    final long sequenceMask;

    RollCycles(String format, int length, int indexCount, int indexSpacing) {
        this.format = format;
        this.length = length;
        this.indexCount = Maths.nextPower2(indexCount, 8);
        this.indexSpacing = Maths.nextPower2(indexSpacing, 1);
        cycleShift = Math.max(32, (Maths.intLog2(indexCount) * 2) + Maths.intLog2(indexSpacing));
        sequenceMask = (1L << cycleShift) - 1;
    }

    @Override
    public String format() {
        return this.format;
    }

    @Override
    public int length() {
        return this.length;
    }

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
        return (int) ((time.currentTimeMillis() - epoch) / length());
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

    public static void main(String[] args) {
        for (RollCycles r : RollCycles.values()) {
            System.out.println(
                    r + ": cycleShift=" + r.cycleShift + " sequenceMask=" + Long.toHexString(r.sequenceMask) + " format=" + r.format);
        }
    }
}
