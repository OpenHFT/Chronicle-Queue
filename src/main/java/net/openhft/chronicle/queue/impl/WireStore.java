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

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

public interface WireStore extends ReferenceCounted, Demarshallable, WriteMarshallable {
    public static final long ROLLED_BIT = 0x4000_0000_0000_0000L;

    WireStore writePosition(long position);

    /**
     * @return an epoch offset as the number of number of milliseconds since January 1, 1970,
     * 00:00:00 GMT, if you set the epoch to the current time, then the cycle will be ZERO
     */
    long epoch();

    /**
     * @return the next writable position, Will be or-ed with ROLLED_BIT if it has rolled.
     */
    long writePosition();

    /**
     * @return the sequence number with the cycle
     */
    long lastEntryIndexed(Wire wire);

    boolean appendRollMeta(@NotNull Wire wire, long cycle);

    long moveToIndex(@NotNull Wire wire, long index);

    @NotNull
    MappedBytes mappedBytes();

    /**
     * Return the sequence number
     *
     * @param wire     to write to.
     * @param position to index
     * @return the excerpt index if known.
     */
    long storeIndexLocation(Wire wire, long position);

    /**
     * Reverse look up an index for a position.
     *
     * @param position of the start of the message
     * @return index in this store.
     */
    long indexForPosition(Wire wire, long position);

    String dump();
}
