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
import net.openhft.chronicle.bytes.MappedFile;
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
     *
     * @return the cycle this store refers to
     */
    long cycle();

    /**
     *
     * @return the first readable position
     */
    long readPosition();

    /**
     *
     * @return the first writable position
     */
    long writePosition();

    /**
     *
     * @return the last index
     */
    long lastIndex();

    /**
     *
     * @param context
     * @param cycle
     * @return
     * @throws IOException
     */
    boolean appendRollMeta(@NotNull WriteContext context, long cycle) throws IOException;

    /**
     *
     * @param context
     * @param marshallable
     * @return
     * @throws IOException
     */
    long append(@NotNull WriteContext context, @NotNull WriteMarshallable marshallable) throws IOException;

    /**
     *
     * @param context
     * @param bytes
     * @return
     * @throws IOException
     */
    long append(@NotNull WriteContext context, @NotNull Bytes bytes) throws IOException;

    /**
     *
     * @param context
     * @param reader
     * @return
     * @throws IOException
     */
    long read(@NotNull ReadContext context, @NotNull ReadMarshallable reader) throws IOException;

    /**
     *
     * @param index
     * @return
     */
    long positionForIndex(long index);

    /**
     *
     * @param mappedFile
     * @param length
     * @param created
     * @param cycle
     * @param builder
     * @param wireSupplier
     * @param closeable
     * @throws IOException
     */
    void install(
        @NotNull MappedFile mappedFile,
        long length,
        boolean created,
        long cycle,
        @NotNull ChronicleQueueBuilder builder,
        @NotNull Function<Bytes, Wire> wireSupplier,
        @Nullable Closeable closeable)
            throws IOException;
}
