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

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingAcceptor;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.wire.*;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class WireBootstrap<D extends Marshallable> implements Closeable {

    private static final long TIMEOUT_MS = 10_000; // 10 seconds.

    private final File masterFile;
    private final Function<Bytes, Wire> wireType;
    private final MappedFile mappedFile;
    private final D delegate;
    private final MappedBytesStore headerStore;
    private final long headerLength;
    private final boolean headerCreated;
    private final MappedBytesStoreFactory<WiredMappedBytesStore> mappedBytesStoreFactory;

    WireBootstrap(
            File masterFile,
            Function<Bytes, Wire> wireType,
            MappedFile mappedFile,
            D delegate,
            MappedBytesStore headerStore,
            long headerLength,
            boolean headerCreated,
            MappedBytesStoreFactory<WiredMappedBytesStore> mappedBytesStoreFactory) {

        this.masterFile = masterFile;
        this.wireType = wireType;
        this.mappedFile = mappedFile;
        this.delegate = delegate;
        this.headerStore = headerStore;
        this.headerLength = headerLength;
        this.headerCreated = headerCreated;
        this.mappedBytesStoreFactory = mappedBytesStoreFactory;
    }

    public File masterFile() {
        return masterFile;
    }

    public MappedFile mappedFile() {
        return mappedFile;
    }

    public D delegate() {
        return delegate;
    }

    public long headerLength() {
        return headerLength;
    }

    public boolean headerCreated() {
        return this.headerCreated;
    }

    public BytesStore store() {
        return this.headerStore;
    }

    public Function<Bytes, Wire> wireSupplier() {
        return this.wireType;
    }

    public Wire acquireWiredChunk(long position) throws IOException {
        WiredMappedBytesStore mappedBytesStore = mappedFile.acquireByteStore(position, mappedBytesStoreFactory);
        return mappedBytesStore.getWire();
    }

    @Override
    public void close() {
        mappedFile.close();
    }

    private static class WiredMappedBytesStore extends MappedBytesStore {
        private final Wire wire;

        WiredMappedBytesStore(ReferenceCounted owner, long start, long address, long capacity, long safeCapacity, Function<Bytes, Wire> wireType) throws IllegalStateException {
            super(owner, start, address, capacity, safeCapacity);
            this.wire = wireType.apply(this.bytesForWrite());
        }

        public Wire getWire() {
            return wire;
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    public static <D extends Marshallable> WireBootstrap<D> build(
        @NotNull File file,
        ThrowingFunction<File, IOException, MappedFile> mappedFileFunction,
        Function<Bytes, Wire> wireType,
        Supplier<D> delegateSupplier,
        ThrowingAcceptor<WireBootstrap<D>, IOException> installer) throws IOException {

        File parentFile = file.getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }

        final MappedFile mappedFile = mappedFileFunction.apply(file);

        final MappedBytesStoreFactory<WiredMappedBytesStore> mappedBytesStoreFactory =
            (owner, start, address, capacity, safeCapacity) ->
                new WiredMappedBytesStore(owner, start, address, capacity, safeCapacity, wireType);

        final WiredMappedBytesStore header = mappedFile.acquireByteStore(0, mappedBytesStoreFactory);

        assert header != null;

        D delegate;
        long length;
        WireBootstrap<D> bootstrap;

        if (header.compareAndSwapInt(0, WireUtil.NOT_INITIALIZED, WireUtil.META_DATA | WireUtil.NOT_READY | WireUtil.UNKNOWN_LENGTH)) {
            Bytes<?> bytes = header.bytesForWrite().writePosition(4);
            wireType.apply(bytes).getValueOut().typedMarshallable(delegate = delegateSupplier.get());
            length = bytes.writePosition();

            bootstrap = new WireBootstrap<>(
                file, wireType, mappedFile, delegate, header, length, true, mappedBytesStoreFactory);

            installer.accept(bootstrap);

            header.writeOrderedInt(0L, WireUtil.META_DATA | Wires.toIntU30(bytes.writePosition() - 4, "Delegate too large"));
        } else {
            long end = System.currentTimeMillis() + TIMEOUT_MS;
            while ((header.readVolatileInt(0) & WireUtil.NOT_READY) == WireUtil.NOT_READY) {
                if (System.currentTimeMillis() > end) {
                    throw new IllegalStateException("Timed out waiting for the header record to be ready in " + file);
                }

                Jvm.pause(1);
            }

            Bytes<?> bytes = header.wire.bytes();
            bytes.readPosition(0);
            bytes.writePosition(bytes.capacity());

            int len = Wires.lengthOf(bytes.readVolatileInt());
            bytes.readLimit(length = bytes.readPosition() + len);
            delegate = wireType.apply(bytes).getValueIn().typedMarshallable();

            bootstrap = new WireBootstrap<>(
                file, wireType, mappedFile, delegate, header, length, false, mappedBytesStoreFactory);

            installer.accept(bootstrap);
        }


        return bootstrap;
    }
}
