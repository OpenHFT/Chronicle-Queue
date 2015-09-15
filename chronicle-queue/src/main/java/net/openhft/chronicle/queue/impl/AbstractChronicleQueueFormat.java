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
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WireUtil;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

public abstract class AbstractChronicleQueueFormat implements ChronicleQueueFormat {

    protected final Function<Bytes, Wire> wireSupplier;

    protected AbstractChronicleQueueFormat(WireType wireType) {
        this.wireSupplier = WireUtil.wireSupplierFor(wireType);
    }

    protected Function<Bytes, Wire> wireSupplier() {
        return this.wireSupplier;
    }

    // *************************************************************************
    // Wire Helpers
    // *************************************************************************

    protected WireOut wireOut(@NotNull Bytes bytes) throws IOException {
        return this.wireSupplier.apply(bytes);
    }

    protected WireOut wireOut(@NotNull MappedFile file, long offset) throws IOException {
        return wireOut(file.acquireBytesForWrite(offset));
    }

    protected WireIn wireIn(@NotNull Bytes bytes) throws IOException {
        return this.wireSupplier.apply(bytes);
    }

    protected WireIn wireIn(@NotNull MappedFile file, long offset) throws IOException {
        return wireIn(file.acquireBytesForRead(offset));
    }
}
