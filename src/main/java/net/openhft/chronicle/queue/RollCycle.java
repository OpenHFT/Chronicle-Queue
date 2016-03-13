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

import net.openhft.chronicle.core.time.TimeProvider;

public interface RollCycle {

    String format();

    int length();

    int defaultIndexCount();

    int defaultIndexSpacing();

    /**
     * @param epoch and EPOCH offset, to all the user to define thier own epoch
     * @return the cycle
     */
    default int current(TimeProvider time, long epoch) {
        return (int) ((time.currentTimeMillis() - epoch) / length());
    }

    long toIndex(int cycle, long sequenceNumber);

    long toSequenceNumber(long index);

    int toCycle(long index);
}
