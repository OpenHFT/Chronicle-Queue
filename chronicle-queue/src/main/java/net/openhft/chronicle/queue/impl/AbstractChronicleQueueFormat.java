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
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

import static net.openhft.chronicle.wire.WireUtil.readMeta;
import static net.openhft.chronicle.wire.WireUtil.writeMeta;

public abstract class AbstractChronicleQueueFormat implements ChronicleQueueFormat {

    public static final long SPB_HEADER_BYTE      = 0;
    public static final long SPB_HEADER_BYTE_SIZE = 8;
    public static final long SPB_HEADER_USET      = 0x0;
    public static final long SPB_HEADER_BUILDING  = 0x1;
    public static final long SPB_HEADER_BUILT     = WireUtil.asLong("QUEUE400");

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

    // *************************************************************************
    //
    // *************************************************************************

    protected void buildHeader(@NotNull BytesStore store, Marshallable header) throws IOException {
        final Bytes rb = store.bytesForRead();
        rb.readPosition(SPB_HEADER_BYTE_SIZE);

        final Bytes wb = store.bytesForWrite();
        wb.writePosition(SPB_HEADER_BYTE_SIZE);

        if(store.compareAndSwapLong(SPB_HEADER_BYTE, SPB_HEADER_USET, SPB_HEADER_BUILDING)) {
            writeMeta(
                wireOut(wb),
                w -> w.write(MetaDataKey.header).typedMarshallable(header)
            );

            if (!store.compareAndSwapLong(SPB_HEADER_BYTE, SPB_HEADER_BUILDING, SPB_HEADER_BUILT)) {
                throw new AssertionError("Concurrent writing of the header");
            }
        }

        waitForTheHeaderToBeBuilt(store);

        readMeta(
            wireIn(rb),
            w -> w.read().marshallable(header)
        );
    }

    protected void waitForTheHeaderToBeBuilt(@NotNull BytesStore store) throws IOException {
        for (int i = 0; i < 1000; i++) {
            long magic = store.readVolatileLong(SPB_HEADER_BYTE);
            if (magic == SPB_HEADER_BUILDING) {
                Jvm.pause(10);
            } else if (magic == SPB_HEADER_BUILT) {
                return;
            } else {
                throw new AssertionError(
                    "Invalid magic number " + Long.toHexString(magic));
            }
        }

        throw new AssertionError("Timeout waiting to build the file");
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

    // *************************************************************************
    //
    // *************************************************************************

    enum MetaDataKey implements WireKey {
        header
    }
}
