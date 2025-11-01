/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.harness;
/*
 * Copyright 2014-2025 chronicle.software
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

import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.rollcycles.RollCycleArithmetic;

import java.util.concurrent.TimeUnit;

public class WeeklyRollCycle implements RollCycle {

    public static final WeeklyRollCycle INSTANCE = new WeeklyRollCycle("yyyyDDD", (int) TimeUnit.DAYS.toMillis(7L), 16384, 16);

    final String format;
    final int length;
    final RollCycleArithmetic arithmetic;

    WeeklyRollCycle(String format, int length, int indexCount, int indexSpacing) {
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
