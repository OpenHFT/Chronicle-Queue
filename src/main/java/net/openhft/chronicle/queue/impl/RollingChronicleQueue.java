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
import net.openhft.chronicle.queue.RollCycle;
import org.jetbrains.annotations.NotNull;

public interface RollingChronicleQueue extends ChronicleQueue {

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
     * @return the first cycle number found, or Integer.MAX_VALUE is none found.
     */
    int firstCycle();

    /**
     * @return the lastCycle available or Integer.MIN_VALUE if none is found.
     */
    int lastCycle();

    /**
     * @return the current cycle
     */
    int cycle();

    RollCycle rollCycle();
}
