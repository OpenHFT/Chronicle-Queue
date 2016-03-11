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
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import org.jetbrains.annotations.NotNull;

public interface RollingChronicleQueue extends ChronicleQueue {

    static long toSequenceNumber(long index) {
        return index & 0xFF_FFFF_FFFFL;
    }

    static int toCycle(long index) {
        // the very last possible entry in a cycle is the value just before the next cycle.
        return (int) (index >>> 40L);
    }

    static long index(long cycle, long sequenceNumber) {
        return (cycle << 40L) + toSequenceNumber(sequenceNumber);
    }

    long epoch();

    /**
     * @param cycle the cycle
     * @param epoch an epoch offset as the number of number of milliseconds since January 1, 1970,
     *              00:00:00 GMT
     * @return the {@code WireStore} associated with this {@code cycle}
     */
    @NotNull
    WireStore storeForCycle(long cycle, final long epoch);

    /**
     * @param store the {@code store} to release
     */
    void release(@NotNull WireStore store);

    /**
     * @return the current cycle
     */
    int cycle();
}
