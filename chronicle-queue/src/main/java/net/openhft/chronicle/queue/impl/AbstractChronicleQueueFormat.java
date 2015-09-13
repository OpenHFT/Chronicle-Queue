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
import net.openhft.chronicle.wire.WireUtil;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;

import java.util.function.Function;

public abstract class AbstractChronicleQueueFormat implements ChronicleQueueFormat {

    private final Function<Bytes, Wire> wireSupplier;

    protected AbstractChronicleQueueFormat(WireType wireType) {
        this.wireSupplier = WireUtil.wireSupplierFor(wireType);
    }

    protected Wire wireFor(Bytes bytes) {
        return wireSupplier.apply(bytes);
    }

    protected Function<Bytes, Wire> wireSupplier() {
        return this.wireSupplier;
    }
}
