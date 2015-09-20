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
package net.openhft.chronicle.wire;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.annotation.ForceInline;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public class WirePool {
    private final ThreadLocal<Wire> readPool;
    private final ThreadLocal<Wire> writePool;
    private final BytesStore bytesStore;
    private final Function<Bytes, Wire> wireSupplier;

    public WirePool(
            @NotNull final BytesStore bytesStore,
            @NotNull final Function<Bytes, Wire> wireSupplier) {

        this.bytesStore = bytesStore;
        this.wireSupplier= wireSupplier;
        this.readPool = new ThreadLocal();
        this.writePool = new ThreadLocal();
    }

    @ForceInline
    public WireIn acquireForReadAt(long position) {
        Wire wire = readPool.get();
        if (wire == null) {
            readPool.set(wire = wireSupplier.apply(bytesStore.bytesForRead()));
        }

        wire.bytes().readPosition(position);
        return wire;
    }

    @ForceInline
    public WireOut acquireForWriteAt(long position) {
        Wire wire = writePool.get();
        if (wire == null) {
            writePool.set(wire = wireSupplier.apply(bytesStore.bytesForWrite()));
        }

        wire.bytes().writePosition(position);
        return wire;
    }
}
