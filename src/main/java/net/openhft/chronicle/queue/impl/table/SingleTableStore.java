/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.single.MetaDataField;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class SingleTableStore<T extends Metadata> implements TableStore<T> {
    public static final String SUFFIX = ".cq4t";

    private static final Logger LOG = LoggerFactory.getLogger(SingleTableStore.class);
    private static final long timeoutMS = Long.getLong("chronicle.table.store.timeoutMS", 10_000);
    @NotNull
    private final WireType wireType;
    @NotNull
    private final T metadata;
    @NotNull
    private final MappedBytes mappedBytes;
    @NotNull
    private final MappedFile mappedFile;
    @NotNull
    private final Wire mappedWire;
    @NotNull
    private final ReferenceCounter refCount;
    private volatile boolean isClosed;

    /**
     * used by {@link Demarshallable}
     *
     * @param wire a wire
     */
    @SuppressWarnings("unused")
    @UsedViaReflection
    private SingleTableStore(@NotNull WireIn wire) {
        assert wire.startUse();
        try {
            this.wireType = Objects.requireNonNull(wire.read(MetaDataField.wireType).object(WireType.class));
            this.mappedBytes = (MappedBytes) (wire.bytes());
            this.mappedFile = mappedBytes.mappedFile();
            this.refCount = ReferenceCounter.onReleased(this::onCleanup);

            wire.consumePadding();
            if (wire.bytes().readRemaining() > 0) {
                this.metadata = Objects.requireNonNull(wire.read(MetaDataField.metadata).typedMarshallable());
            } else {
                //noinspection unchecked
                this.metadata = (T) Metadata.NoMeta.INSTANCE;
            }

            mappedWire = wireType.apply(mappedBytes);
        } finally {
            assert wire.endUse();
        }
    }

    /**
     * @param wireType    the wire type that is being used
     * @param mappedBytes used to mapped the data store file
     */
    SingleTableStore(@NotNull final WireType wireType,
                     @NotNull MappedBytes mappedBytes,
                     @NotNull T metadata) {
        this.wireType = wireType;
        this.metadata = metadata;
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        this.refCount = ReferenceCounter.onReleased(this::onCleanup);
        mappedWire = wireType.apply(mappedBytes);
    }

    public static <T, R> R doWithSharedLock(File file, Function<T, ? extends R> code, Supplier<T> target) {
        final long timeoutAt = System.currentTimeMillis() + timeoutMS;
        boolean warnedOnFailure = false;
        try (final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            while (System.currentTimeMillis() < timeoutAt) {
                try {
                    FileLock fileLock = channel.tryLock( 0L, Long.MAX_VALUE, true );
                    if (fileLock != null) {
                        return code.apply(target.get());
                    }
                } catch (IOException | OverlappingFileLockException e) {
                    // failed to acquire the lock, wait until other operation completes
                    if (!warnedOnFailure) {
                        LOG.debug("Failed to acquire a lock on the table store file. Retrying");
                        warnedOnFailure = true;
                    }
                }
                Jvm.pause(50L);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't perform operation with file lock", e);
        }
        throw new IllegalStateException("Unable to claim shared lock on file " + file);
    }

    public static <T, R> R doWithExclusiveLock(File file, Function<T, ? extends R> code, Supplier<T> target) {
        final long timeoutAt = System.currentTimeMillis() + timeoutMS;
        boolean warnedOnFailure = false;
        try (final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
            while (System.currentTimeMillis() < timeoutAt) {
                try {
                    FileLock fileLock = channel.tryLock();
                    if (fileLock != null) {
                        return code.apply(target.get());
                    }
                } catch (IOException | OverlappingFileLockException e) {
                    // failed to acquire the lock, wait until other operation completes
                    if (!warnedOnFailure) {
                        LOG.debug("Failed to acquire a lock on the table store file. Retrying");
                        warnedOnFailure = true;
                    }
                }
                Jvm.pause(50L);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't perform operation with file lock", e);
        }
        throw new IllegalStateException("Unable to claim exclusive lock on file " + file);
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @NotNull
    @Override
    public File file() {
        return mappedFile.file();
    }

    @NotNull
    @Override
    public String dump() {

        MappedBytes bytes = MappedBytes.mappedBytes(mappedFile);
        try {
            bytes.readLimit(bytes.realCapacity());
            return Wires.fromSizePrefixedBlobs(bytes);
        } finally {
            bytes.release();
        }
    }

    @Override
    public void reserve() throws IllegalStateException {
        this.refCount.reserve();
    }

    @Override
    public void release() throws IllegalStateException {
        this.refCount.release();
    }

    @Override
    public long refCount() {
        return this.refCount.get();
    }

    @Override
    public boolean tryReserve() {
        return refCount.tryReserve();
    }

    @Override
    public void close() {

        while (refCount.get() > 0) {
            refCount.release();
        }
        isClosed = true;
    }

    /**
     * @return creates a new instance of mapped bytes, because, for example the tailer and appender
     * can be at different locations.
     */
    @NotNull
    @Override
    public MappedBytes bytes() {
        return MappedBytes.mappedBytes(mappedFile);
    }

    @NotNull
    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "wireType=" + wireType +
                ", mappedFile=" + mappedFile +
                ", refCount=" + refCount +
                '}';
    }

    // *************************************************************************
    // Marshalling
    // *************************************************************************

    private void onCleanup() {
        mappedBytes.release();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {

        wire.write(MetaDataField.wireType).object(wireType);

        if (metadata != Metadata.NoMeta.INSTANCE)
            wire.write(MetaDataField.metadata).typedMarshallable(this.metadata);

        // align to a word whether needed or not as a micro-optimisation.
        wire.writeAlignTo(Integer.BYTES, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized LongValue acquireValueFor(CharSequence key, long defaultValue) { // TODO Change to ThreadLocal values if performance is a problem.
        StringBuilder sb = Wires.acquireStringBuilder();
        mappedBytes.reserve();
        try {
            mappedBytes.readPosition(0);
            mappedBytes.readLimit(mappedBytes.realCapacity());
            while (mappedWire.readDataHeader()) {
                int header = mappedBytes.readVolatileInt();
                if (Wires.isNotComplete(header))
                    break;
                long readPosition = mappedBytes.readPosition();
                int length = Wires.lengthOf(header);
                ValueIn valueIn = mappedWire.readEventName(sb);
                if (StringUtils.equalsCaseIgnore(key, sb)) {
                    return valueIn.int64ForBinding(null);
                }
                mappedBytes.readPosition(readPosition + length);
            }
            // not found
            int safeLength = Maths.toUInt31(mappedBytes.realCapacity() - mappedBytes.readPosition());
            mappedBytes.writeLimit(mappedBytes.realCapacity());
            mappedBytes.writePosition(mappedBytes.readPosition());
            long pos = mappedWire.enterHeader(safeLength);
            LongValue longValue = wireType.newLongReference().get();
            mappedWire.writeEventName(key).int64forBinding(defaultValue, longValue);
            mappedWire.writeAlignTo(Integer.BYTES, 0);
            mappedWire.updateHeader(pos, false, 0);
            return longValue;

        } catch (StreamCorruptedException | EOFException e) {
            throw new IORuntimeException(e);

        } finally {
            mappedBytes.release();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R doWithExclusiveLock(Function<TableStore<T>, ? extends R> code) {
        return doWithExclusiveLock(file(), code, () -> this);
    }

    @Override
    public T metadata() {
        return metadata;
    }
}

