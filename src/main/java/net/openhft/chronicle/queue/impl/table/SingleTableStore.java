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
import net.openhft.chronicle.queue.impl.single.SimpleStoreRecovery;
import net.openhft.chronicle.queue.impl.single.StoreRecovery;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.function.Function;

public class SingleTableStore implements TableStore {
    private static final long timeoutMS = Long.getLong("chronicle.table.store.timeoutMS", 10_000);
    @NotNull
    private final WireType wireType;
    @NotNull
    private final MappedBytes mappedBytes;
    @NotNull
    private final MappedFile mappedFile;
    @NotNull
    private final Wire mappedWire;
    @NotNull
    private final ReferenceCounter refCount;
    @NotNull
    private final StoreRecovery recovery;

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
            this.wireType = wire.read(MetaDataField.wireType).object(WireType.class);
            assert wireType != null;

            this.mappedBytes = (MappedBytes) (wire.bytes());
            this.mappedFile = mappedBytes.mappedFile();
            this.refCount = ReferenceCounter.onReleased(this::onCleanup);

            if (wire.bytes().readRemaining() > 0) {
                this.recovery = wire.read(MetaDataField.recovery).typedMarshallable();
            } else {
                this.recovery = new SimpleStoreRecovery(); // disabled.
            }
            mappedWire = wireType.apply(mappedBytes);
        } finally {
            assert wire.endUse();
        }
    }

    /**
     * @param wireType    the wire type that is being used
     * @param mappedBytes used to mapped the data store file
     * @param recovery    used to recover from concurrent modifications
     */
    public SingleTableStore(@NotNull final WireType wireType,
                            @NotNull MappedBytes mappedBytes,
                            @NotNull StoreRecovery recovery) {
        this.recovery = recovery;
        this.wireType = wireType;
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        this.refCount = ReferenceCounter.onReleased(this::onCleanup);
        mappedWire = wireType.apply(mappedBytes);
    }

    /**
     * @return the type of wire used
     */
    @NotNull
    @Override
    public WireType wireType() {
        return wireType;
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
    public void close() {
        while (refCount.get() > 0) {
            refCount.release();
        }
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

    private void onCleanup() {
        mappedBytes.release();
    }

    // *************************************************************************
    // Marshalling
    // *************************************************************************

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {

        wire.write(MetaDataField.wireType).object(wireType);
        wire.write(MetaDataField.recovery).typedMarshallable(recovery);
        wire.padToCacheAlign();
    }

    @Override
    public long writeHeader(@NotNull Wire wire, int length, int safeLength, long timeoutMS) throws EOFException, UnrecoverableTimeoutException {
        return recovery.writeHeader(wire, length, safeLength, timeoutMS, null, null);
    }

    @Override
    public long tryWriteHeader(@NotNull Wire wire, int length, int safeLength) {
        return recovery.tryWriteHeader(wire, length, safeLength);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized LongValue acquireValueFor(CharSequence key) { // TODO Change to ThreadLocal values if performance is a problem.
        StringBuilder sb = Wires.acquireStringBuilder();
        mappedBytes.reserve();
        try {
            mappedBytes.readPosition(0);
            mappedBytes.readLimit(mappedBytes.realCapacity());
            while (mappedWire.readDataHeader()) {
                int header = mappedBytes.readInt();
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
            long pos = recovery.writeHeader(mappedWire, Wires.UNKNOWN_LENGTH, safeLength, timeoutMS, null, null);
            LongValue longValue = wireType.newLongReference().get();
            mappedWire.writeEventName(key).int64forBinding(Long.MIN_VALUE, longValue);
            mappedWire.updateHeader(pos, false);
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
    public <R> R doWithExclusiveLock(Function<TableStore, ? extends R> code) {
        final long timeoutAt = System.currentTimeMillis() + 2 * timeoutMS;
        boolean warnedOnFailure = false;
        try (final FileChannel channel = FileChannel.open(file().toPath(), StandardOpenOption.WRITE)) {
            while (System.currentTimeMillis() < timeoutAt) {
                try {
                    FileLock fileLock = channel.tryLock();
                    if (fileLock != null) {
                        return code.apply(this);
                    }
                } catch (IOException | OverlappingFileLockException e) {
                    // failed to acquire the lock, wait until other operation completes
                    if (!warnedOnFailure) {
                        Jvm.warn().on(getClass(), "Failed to acquire a lock on the table store file. Retrying", e);
                        warnedOnFailure = true;
                    }
                }
                Jvm.pause(50L);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't open table store file for writing", e);
        }
        throw new IllegalStateException("Unable to claim exclusive lock on file " + file());
    }
}

