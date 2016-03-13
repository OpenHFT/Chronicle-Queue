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
import net.openhft.chronicle.queue.impl.single.ScanResult;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.util.concurrent.TimeoutException;

public interface WireStore extends ReferenceCounted, Demarshallable, WriteMarshallable {
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
    long lastEntryIndexed(Wire wire, long timeoutMS);

    boolean appendRollMeta(@NotNull Wire wire, long cycle, long timeoutMS) throws TimeoutException;

    ScanResult moveToIndex(@NotNull Wire wire, long index, long timeoutMS) throws TimeoutException;

    @NotNull
    MappedBytes mappedBytes();

    /**
     * Reverse look up an index for a position.
     *
     * @param position of the start of the message
     * @param timeoutMS
     * @return index in this store.
     */
    long indexForPosition(Wire wire, long position, long timeoutMS) throws EOFException, TimeoutException;

    String dump();
}
