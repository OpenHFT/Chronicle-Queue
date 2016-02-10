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

    long MINUS_1_40BIT = toSequenceNumber(-1);

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
    long cycle();



    static long toSequenceNumber(long index) {
        final long l = index & 0xFFFFFFFFFFL;
        return (l == MINUS_1_40BIT) ? -1 : l;
    }

    static long toCycle(long index) {
        int result = (int) (index >> 40L);
        if (result > (1 << 24))
            throw new IllegalStateException("cycle value is too large, it must fit into 24bits, " +
                    "either use a larger rollType of increase the roll offset.");

        if (result < 0)
            throw new IllegalStateException("Invalid cycle=" + result + ", cycles can not be negative" +
                    ".");
        return result;
    }

    static long index(long cycle, long sequenceNumber) {
        return (cycle << 40L) + (sequenceNumber & 0xFFFFFFFFFFL);
    }
}
