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
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

public interface WireStore extends ReferenceCounted, Marshallable {

    /**
     * @return the cycle this store refers to
     */
    long cycle();

    /**
     * @return an epoc offset as the number of number of milliseconds since January 1, 1970,
     * 00:00:00 GMT
     */
    long epoc();

    /**
     * @return the first readable position
     */
    // long readPosition();

    /**
     * @return the first writable position
     */
    long writePosition();

    /**
     * @return the last index
     */
    long lastIndex();

    boolean appendRollMeta(@NotNull Wire context, long cycle) throws IOException;

    long append(@NotNull Wire wire, @NotNull WriteMarshallable marshallable) throws IOException;

    long append(@NotNull Wire wire, @NotNull WriteBytesMarshallable marshallable) throws IOException;

    long append(@NotNull Wire wire, @NotNull Bytes bytes) throws IOException;

    long read(@NotNull Wire wire, @NotNull ReadMarshallable reader) throws IOException;

    long read(@NotNull Wire v, @NotNull ReadBytesMarshallable reader) throws IOException;

    long moveToIndex(@NotNull Wire wire, long index);

    void install(
            @NotNull MappedBytes mappedBytes,
            long length,
            boolean created,
            long cycle,
            @NotNull ChronicleQueueBuilder builder,
            @NotNull Function<Bytes, Wire> wireSupplier,
            @Nullable Closeable closeable)
            throws IOException;

    MappedBytes mappedBytes();

}
