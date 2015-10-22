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
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public class WirePool {
    private final ThreadLocal<Wire> readPool;
    private final ThreadLocal<Wire> writePool;
    private final long blockSize;


    public WirePool(long blockSize, @NotNull final Function<Bytes, Wire> wireSupplier) {

        this.blockSize = blockSize;
        this.readPool  = ThreadLocal.withInitial(() -> wireSupplier.apply(VanillaBytes.vanillaBytes()));
        this.writePool = ThreadLocal.withInitial(() -> wireSupplier.apply(VanillaBytes.vanillaBytes()));
    }

    @ForceInline
    public WireIn acquireForRead(@NotNull BytesStore store, long position) {
        final Wire wire = readPool.get();
        final VanillaBytes bytes = (VanillaBytes) wire.bytes();

        bytes.bytesStore(store, position, this.blockSize);
        bytes.readPosition(position);
        bytes.readLimit(position + this.blockSize);

        return wire;
    }

    @ForceInline
    public WireOut acquireForWrite(@NotNull BytesStore store, long position) {
        final Wire wire = writePool.get();
        final VanillaBytes bytes = (VanillaBytes) wire.bytes();

        bytes.bytesStore(store, position, this.blockSize);
        bytes.writePosition(position);
        bytes.writeLimit(position + this.blockSize);

        return wire;
    }
}
