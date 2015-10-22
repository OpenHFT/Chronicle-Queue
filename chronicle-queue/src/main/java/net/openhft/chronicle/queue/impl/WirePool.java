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
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

public class WirePool {
    private final ThreadLocal<Wire> readPool;
    private final ThreadLocal<Wire> writePool;
    private final MappedFile mappedFile;
    private final Function<Bytes, Wire> wireSupplier;

    public WirePool(
            @NotNull final MappedFile mappedFile,
            @NotNull final Function<Bytes, Wire> wireSupplier) {

        this.mappedFile = mappedFile;
        this.wireSupplier= wireSupplier;

        this.readPool = ThreadLocal.withInitial(() -> {
            try {
                return wireSupplier.apply(new VanillaBytes(mappedFile.acquireByteStore(0)));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });

        this.writePool = ThreadLocal.withInitial(() -> {
            try {
                return wireSupplier.apply(new VanillaBytes(mappedFile.acquireByteStore(0)));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @ForceInline
    public WireIn acquireForReadAt(long position) {
        try {
            final Wire wire = readPool.get();
            mappedFile.acquireBytesForRead(position, (VanillaBytes) wire.bytes());

            return wire;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @ForceInline
    public WireOut acquireForWriteAt(long position) {
        try {
            final Wire wire = writePool.get();
            mappedFile.acquireBytesForWrite(position, (VanillaBytes) wire.bytes());

            return wire;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
