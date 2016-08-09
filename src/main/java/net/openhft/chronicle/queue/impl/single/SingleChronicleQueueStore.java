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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.StreamCorruptedException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SingleChronicleQueueStore implements WireStore {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(SCQIndexing.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(SCQRoll.class, "Roll");
    }

    @NotNull
    final SCQIndexing indexing;
    @NotNull
    private final WireType wireType;
    @NotNull
    private final SCQRoll roll;
    @NotNull
    private final LongValue writePosition;
    private final MappedBytes mappedBytes;
    private final MappedFile mappedFile;
    @NotNull
    private final ReferenceCounter refCount;
    private final StoreRecovery recovery;

    @Nullable
    private LongValue lastAcknowledgedIndexReplicated;

    /**
     * used by {@link net.openhft.chronicle.wire.Demarshallable}
     *
     * @param wire a wire
     */
    @UsedViaReflection
    private SingleChronicleQueueStore(WireIn wire) {
        assert wire.startUse();
        try {
            this.wireType = wire.read(MetaDataField.wireType).object(WireType.class);
            assert wireType != null;
            this.writePosition = wire.newLongReference();
            wire.read(MetaDataField.writePosition).int64(writePosition);
            this.roll = wire.read(MetaDataField.roll).typedMarshallable();

            this.mappedBytes = (MappedBytes) (wire.bytes());
            this.mappedFile = mappedBytes.mappedFile();
            this.refCount = ReferenceCounter.onReleased(this::onCleanup);
            this.indexing = wire.read(MetaDataField.indexing).typedMarshallable();
            assert indexing != null;
            this.indexing.writePosition = writePosition;

            if (wire.bytes().readRemaining() > 0) {
                this.lastAcknowledgedIndexReplicated = wire.read(MetaDataField.lastAcknowledgedIndexReplicated)
                        .int64ForBinding(null);
            } else {
                this.lastAcknowledgedIndexReplicated = null; // disabled.
            }
            if (wire.bytes().readRemaining() > 0) {
                this.recovery = wire.read(MetaDataField.recovery)
                        .typedMarshallable();
            } else {
                this.recovery = new SimpleStoreRecovery(); // disabled.
            }
        } finally {
            assert wire.endUse();
        }
    }

    /**
     * @param rollCycle    the current rollCycle
     * @param wireType     the wire type that is being used
     * @param mappedBytes  used to mapped the data store file
     * @param epoch        sets an epoch offset as the number of number of milliseconds since
     * @param indexCount   the number of entries in each index.
     * @param indexSpacing the spacing between indexed entries.
     * @param recovery
     */
    SingleChronicleQueueStore(@Nullable RollCycle rollCycle,
                              @NotNull final WireType wireType,
                              @NotNull MappedBytes mappedBytes,
                              long epoch,
                              int indexCount,
                              int indexSpacing,
                              StoreRecovery recovery) {
        this.recovery = recovery;
        this.roll = new SCQRoll(rollCycle, epoch);
        this.wireType = wireType;
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        this.refCount = ReferenceCounter.onReleased(this::onCleanup);

        indexCount = Maths.nextPower2(indexCount, 8);
        indexSpacing = Maths.nextPower2(indexSpacing, 1);

        this.indexing = new SCQIndexing(wireType, indexCount, indexSpacing);
        this.indexing.writePosition = this.writePosition = wireType.newLongReference().get();
        this.lastAcknowledgedIndexReplicated = wireType.newLongReference().get();
    }

    public static void dumpStore(Wire wire) {
        Bytes<?> bytes = wire.bytes();
        bytes.readPositionUnlimited(0);
        Jvm.debug().on(SingleChronicleQueueStore.class, Wires.fromSizePrefixedBlobs(wire));
    }

    public static String dump(String directoryFilePath) {
        SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(directoryFilePath).build();
        return q.dump();
    }

    @Override
    public File file() {
        return mappedFile == null ? null : mappedFile.file();
    }

    /**
     * when using replication to another host, this is the last index that has been confirmed to *
     * have been read by the remote host.
     */
    public long lastAcknowledgedIndexReplicated() {
        return lastAcknowledgedIndexReplicated == null ? -1 : lastAcknowledgedIndexReplicated.getVolatileValue();
    }

    public void lastAcknowledgedIndexReplicated(long newValue) {
        if (lastAcknowledgedIndexReplicated != null)
            lastAcknowledgedIndexReplicated.setMaxValue(newValue);
    }

    @Override
    public String dump() {
        mappedFile.reserve();
        MappedBytes bytes = MappedBytes.mappedBytes(mappedFile);
        try {
            bytes.readLimit(bytes.realCapacity());
            return Wires.fromSizePrefixedBlobs(bytes);
        } finally {
            mappedFile.release();
        }
    }

    @Override
    public long writePosition() {
        return this.writePosition.getVolatileValue();
    }

    @Override
    public WireStore writePosition(long position) {

        assert writePosition.getVolatileValue() + mappedFile.chunkSize() > position;
        int header = mappedBytes.readVolatileInt(position);
        if (Wires.isReadyData(header))
            writePosition.setMaxValue(position);
        else
            throw new AssertionError();
        return this;
    }

    /**
     * @return an epoch offset as the number of number of milliseconds since January 1, 1970,
     * 00:00:00 GMT
     */
    @Override
    public long epoch() {
        return this.roll.epoch();
    }

    /**
     * Moves the position to the index
     *
     * @param ec    the data structure we are navigating
     * @param index the index we wish to move to
     * @return whether the index was found for reading.
     */
    @Override
    public ScanResult moveToIndexForRead(@NotNull ExcerptContext ec, long index) {
        try {
            return indexing.moveToIndex(recovery, ec, index);
        } catch (UnrecoverableTimeoutException | StreamCorruptedException e) {
            return ScanResult.NOT_REACHED;
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
        mappedFile.reserve();
        return MappedBytes.mappedBytes(mappedFile);
    }

    @Override
    public long sequenceForPosition(final ExcerptContext ec, final long position, boolean inclusive) throws
            EOFException, UnrecoverableTimeoutException, StreamCorruptedException {
        return indexing.sequenceForPosition(recovery, ec, position, inclusive);
    }

    @Override
    public String toString() {
        return "SingleChronicleQueueStore{" +
                "indexing=" + indexing +
                ", wireType=" + wireType +
                ", roll=" + roll +
                ", writePosition=" + writePosition +
                ", mappedFile=" + mappedFile +
                ", refCount=" + refCount +
                ", lastAcknowledgedIndexReplicated=" + lastAcknowledgedIndexReplicated +
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
        if (lastAcknowledgedIndexReplicated == null)
            lastAcknowledgedIndexReplicated = wire.newLongReference();

        wire.write(MetaDataField.wireType).object(wireType)
                .write(MetaDataField.writePosition).int64forBinding(0L, writePosition)
                .write(MetaDataField.roll).typedMarshallable(this.roll)
                .write(MetaDataField.indexing).typedMarshallable(this.indexing)
                .write(MetaDataField.lastAcknowledgedIndexReplicated)
                .int64forBinding(-1L, lastAcknowledgedIndexReplicated);
        wire.write(MetaDataField.recovery).typedMarshallable(recovery);
    }

    @Override
    public void setPositionForSequenceNumber(final ExcerptContext ec, long sequenceNumber,
                                             long position)
            throws UnrecoverableTimeoutException, StreamCorruptedException {
        long nextSequence = indexing.nextEntryToBeIndexed();
        if (nextSequence > sequenceNumber)
            return;

        try {
            indexing.setPositionForSequenceNumber(recovery, ec,
                    sequenceNumber, position);

        } catch (EOFException ignored) {
            // todo unable to add an index to a rolled store.
        }
    }

    @Override
    public long writeHeader(Wire wire, int length, long timeoutMS) throws EOFException, UnrecoverableTimeoutException {
        return recovery.writeHeader(wire, length, timeoutMS, writePosition);
    }

    @Override
    public void writeEOF(Wire wire, long timeoutMS) throws UnrecoverableTimeoutException {
        try {
            wire.writeEndOfWire(timeoutMS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            recovery.writeEndOfWire(wire, timeoutMS);
        }
    }

    enum MetaDataField implements WireKey {
        wireType,
        writePosition,
        roll,
        indexing,
        lastAcknowledgedIndexReplicated,
        recovery;

        @Nullable
        @Override
        public Object defaultValue() {
            throw new IORuntimeException("field " + name() + " required");
        }
    }
}

