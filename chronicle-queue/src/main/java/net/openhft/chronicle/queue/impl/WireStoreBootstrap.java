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
import net.openhft.chronicle.bytes.MappedBytesStore;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireUtil;
import net.openhft.chronicle.wire.Wires;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class WireStoreBootstrap implements Closeable {

    private static final long TIMEOUT_MS = 10_000; // 10 seconds.

    private final File masterFile;
    private final Function<Bytes, Wire> wireType;
    private final MappedFile mappedFile;
    private final WireStore delegate;
    private final MappedBytesStore header;
    private final long headerLength;

    WireStoreBootstrap(
        File masterFile,
        Function<Bytes, Wire> wireType,
        MappedFile mappedFile,
        WireStore delegate,
        MappedBytesStore header,
        long length) {

        this.masterFile = masterFile;
        this.wireType = wireType;
        this.mappedFile = mappedFile;
        this.delegate = delegate;
        this.header = header;
        this.headerLength = length;
    }

    public static WireStore build(
            File file,
            ThrowingFunction<File, IOException, MappedFile> mappedFileFunction,
            Function<Bytes, Wire> wireType,
            Supplier<WireStore> delegateSupplier) throws IOException {

        File parentFile = file.getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }

        final MappedFile mf = mappedFileFunction.apply(file);
        final BytesStore bs = mf.acquireByteStore(0L);

        WireStore delegate;

        if (bs.compareAndSwapInt(0, WireUtil.NOT_INITIALIZED, WireUtil.META_DATA | WireUtil.NOT_READY | WireUtil.UNKNOWN_LENGTH)) {
            Bytes<?> bytes = bs.bytesForWrite();
            bytes.writePosition(4);

            wireType.apply(bytes).getValueOut().typedMarshallable(delegate = delegateSupplier.get());
            bs.writeOrderedInt(0L, WireUtil.META_DATA | Wires.toIntU30(bytes.writePosition() - 4, "Delegate too large"));

            delegate.install(bs, bytes.writePosition(), 0);
        } else {
            long end = System.currentTimeMillis() + TIMEOUT_MS;
            while ((bs.readVolatileInt(0) & WireUtil.NOT_READY) == WireUtil.NOT_READY) {
                if (System.currentTimeMillis() > end) {
                    throw new IllegalStateException("Timed out waiting for the header record to be ready in " + file);
                }

                Jvm.pause(1);
            }

            Bytes<?> bytes = bs.bytesForRead();
            bytes.readPosition(0);
            bytes.writePosition(bytes.capacity());
            bytes.readLimit(Wires.lengthOf(bytes.readVolatileInt(0)));

            delegate = wireType.apply(bytes).getValueIn().typedMarshallable();
            delegate.install(bs);
        }


        return delegate;
    }

    @Override
    public void close() {
        mappedFile.close();
    }
}
