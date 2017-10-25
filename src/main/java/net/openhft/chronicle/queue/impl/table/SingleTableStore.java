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

import net.openhft.chronicle.bytes.Bytes;
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
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.StoreRecovery;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.StreamCorruptedException;

public class SingleTableStore implements TableStore {
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
    @Nullable
    private final StoreRecovery recovery;

    /**
     * used by {@link Demarshallable}
     *
     * @param wire a wire
     */
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
                this.recovery = wire.read(MetaDataField.recovery)
                        .typedMarshallable();
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
     * @param recovery
     */
    public SingleTableStore(@NotNull final WireType wireType,
                            @NotNull MappedBytes mappedBytes,
                            StoreRecovery recovery) {
        this.recovery = recovery;
        this.wireType = wireType;
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        this.refCount = ReferenceCounter.onReleased(this::onCleanup);
        mappedWire = wireType.apply(mappedBytes);
    }

    public static void dumpStore(@NotNull Wire wire) {
        Bytes<?> bytes = wire.bytes();
        bytes.readPositionUnlimited(0);
        Jvm.debug().on(SingleTableStore.class, Wires.fromSizePrefixedBlobs(wire));
    }

    @NotNull
    public static String dump(@NotNull String directoryFilePath) {
        SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(directoryFilePath).build();
        return q.dump();
    }

    /**
     * @return the type of wire used
     */
    @NotNull
    @Override
    public WireType wireType() {
        return wireType;
    }

    @Nullable
    @Override
    public File file() {
        return mappedFile == null ? null : mappedFile.file();
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
        return recovery.writeHeader(wire, length, safeLength, timeoutMS, null);
    }


    // TODO Change to ThreadLocal values if performance is a problem.
    @Override
    public synchronized LongValue acquireValueFor(CharSequence key) {
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
            long pos = recovery.writeHeader(mappedWire, Wires.UNKNOWN_LENGTH, safeLength, 10_000, null);
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
}

