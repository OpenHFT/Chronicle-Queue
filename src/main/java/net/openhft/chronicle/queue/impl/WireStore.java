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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.function.Function;

public interface WireStore extends ReferenceCounted, Marshallable {

    void writePosition(long position);

    /**
     * @return the cycle this store refers to, this is based on the time offset by {@code epoch} and
     * calculated using the roll type
     */
    long cycle();

    /**
     * @return an epoch offset as the number of number of milliseconds since January 1, 1970,
     * 00:00:00 GMT, if you set the epoch to the current time, then the cycle will be ZERO
     */
    long epoch();

    /**
     * @return the next writable position
     */
    long writePosition();

    /**
     * @return the sequence number with the cycle
     */
    long sequenceNumber();

    long firstSequenceNumber();

    boolean appendRollMeta(@NotNull Wire wire, long cycle);

    long moveToIndex(@NotNull Wire wire, long index);

    void install(
            long length,
            boolean created,
            long cycle,
            @NotNull ChronicleQueueBuilder builder,
            @NotNull Function<Bytes, Wire> wireSupplier,
            @Nullable Closeable closeable)
            ;

    @NotNull
    MappedBytes mappedBytes();


    void storeIndexLocation(Wire wire, long position, long index);
}
