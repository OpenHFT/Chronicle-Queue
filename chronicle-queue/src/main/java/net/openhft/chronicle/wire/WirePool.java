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
import net.openhft.chronicle.core.annotation.ForceInline;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;
import java.util.function.Supplier;

public class WirePool {
    private final ThreadLocal<Wire> pool;
    private final Supplier<Bytes> bytesSupplier;
    private final Function<Bytes, Wire> wireSupplier;

    public WirePool(
            @NotNull final Supplier<Bytes> bytesSupplier,
            @NotNull final Function<Bytes, Wire> wireSupplier) {

        this.pool = new ThreadLocal();
        this.bytesSupplier = bytesSupplier;
        this.wireSupplier= wireSupplier;
    }

    @ForceInline
    public WireIn acquireForReadAt(long position) {
        Wire wire = pool.get();
        if (wire == null) {
            pool.set(wire = wireSupplier.apply(bytesSupplier.get()));
        }

        wire.bytes().readPosition(position);
        return wire;
    }

    @ForceInline
    public WireOut acquireForWriteAt(long position) {
        Wire wire = pool.get();
        if (wire == null) {
            pool.set(wire = wireSupplier.apply(bytesSupplier.get()));
        }

        wire.bytes().writePosition(position);
        return wire;
    }
}
