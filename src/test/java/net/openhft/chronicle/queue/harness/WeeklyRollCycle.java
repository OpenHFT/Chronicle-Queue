package net.openhft.chronicle.queue.harness;
/*
 * Copyright 2014-2020 chronicle.software
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

public class WeeklyRollCycle implements RollCycle {

    public static final WeeklyRollCycle INSTANCE = new WeeklyRollCycle("yyyyDDD", (int) TimeUnit.DAYS.toMillis(7L), 16384, 16);

    final String format;
    final int length;
    final int cycleShift;
    final int indexCount;
    final int indexSpacing;
    final long sequenceMask;

    WeeklyRollCycle(String format, int length, int indexCount, int indexSpacing) {
        this.format = format;
        this.length = length;
        this.indexCount = Maths.nextPower2(indexCount, 8);
        this.indexSpacing = Maths.nextPower2(indexSpacing, 1);
        cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));
        sequenceMask = (1L << cycleShift) - 1;
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

    @Override
    public long maxMessagesPerCycle() {
        return RollCycles.maxMessagesPerCycle(indexCount, indexSpacing);
    }

}
