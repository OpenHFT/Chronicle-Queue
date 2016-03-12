/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Maths;

public enum RollCycles implements RollCycle {
    SECONDLY("yyyyMMdd-HHmmss", 1000, 8 << 10, 16),
    MINUTELY("yyyyMMdd-HHmm", 60 * 1000, 32 << 10, 32),
    HOURLY("yyyyMMdd-HH", 60 * 60 * 1000, 128 << 10, 64),
    DAILY("yyyyMMdd", 24 * 60 * 60 * 1000, 128 << 10, 64);

    final String format;
    final int length;
    final int cycleShift;
    final int indexCount;
    final int indexSpacing;

    RollCycles(String format, int length, int indexCount, int indexSpacing) {
        this.format = format;
        this.length = length;
        this.indexCount = Maths.nextPower2(indexCount, 64);
        this.indexSpacing = Maths.nextPower2(indexSpacing, 1);
        cycleShift = Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing);
    }

    @Override
    public String format() {
        return this.format;
    }

    @Override
    public int length() {
        return this.length;
    }

    public int defaultIndexCount() {
        return indexCount;
    }

    public int defaultIndexSpacing() {
        return indexSpacing;
    }

    @Override
    public long toIndex(int cycle, long sequenceNumber) {
        // todo fix for SECONDLY, MINUTELY
        return ((long) cycle << 40) + (sequenceNumber & ((1L << 40) - 1));
    }
}
