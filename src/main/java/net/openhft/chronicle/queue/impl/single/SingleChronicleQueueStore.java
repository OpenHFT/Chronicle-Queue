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

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.core.values.TwoLongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class SingleChronicleQueueStore implements WireStore {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(SCQIndexing.class);
    }

    @NotNull
    final SCQIndexing indexing;
    @NotNull
    private final LongValue writePosition;
    @NotNull
    private final MappedBytes mappedBytes;
    @NotNull
    private final MappedFile mappedFile;
    @NotNull
    private final ReferenceCounter refCount;
    private final int dataVersion;

    @NotNull
    private final transient Sequence sequence;

    private volatile Thread lastAccessedThread;

    /**
     * used by {@link net.openhft.chronicle.wire.Demarshallable}
     *
     * @param wire a wire
     */
    @UsedViaReflection
    private SingleChronicleQueueStore(@NotNull WireIn wire) {
        assert wire.startUse();
        try {
            writePosition = loadWritePosition(wire);
            this.mappedBytes = (MappedBytes) (wire.bytes());
            this.mappedFile = mappedBytes.mappedFile();
            this.refCount = ReferenceCounter.onReleased(this::onCleanup);
            this.indexing = Objects.requireNonNull(wire.read(MetaDataField.indexing).typedMarshallable());
            this.indexing.writePosition = writePosition;
            this.sequence = new RollCycleEncodeSequence(writePosition, rollIndexCount(), rollIndexSpacing());
            this.indexing.sequence = sequence;
            if (wire.bytes().readRemaining() > 0) {
                final int version = wire.read(MetaDataField.dataFormat).int32();
                this.dataVersion = version > 1 ? 0 : version;
            } else
                this.dataVersion = 0;

        } finally {
            assert wire.endUse();
        }
    }

    /**
     * @param rollCycle    the current rollCycle
     * @param wireType     the wire type that is being used
     * @param mappedBytes  used to mapped the data store file
     * @param indexCount   the number of entries in each index.
     * @param indexSpacing the spacing between indexed entries.
     */
    public SingleChronicleQueueStore(@NotNull RollCycle rollCycle,
                                     @NotNull final WireType wireType,
                                     @NotNull MappedBytes mappedBytes,
                                     int indexCount,
                                     int indexSpacing) {
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        this.refCount = ReferenceCounter.onReleased(this::onCleanup);

        indexCount = Maths.nextPower2(indexCount, 8);
        indexSpacing = Maths.nextPower2(indexSpacing, 1);

        this.indexing = new SCQIndexing(wireType, indexCount, indexSpacing);
        this.indexing.writePosition = this.writePosition = wireType.newTwoLongReference().get();
        this.indexing.sequence = this.sequence = new RollCycleEncodeSequence(writePosition,
                rollCycle.defaultIndexCount(),
                rollCycle.defaultIndexSpacing());
        this.dataVersion = 1;
    }

    @NotNull
    public static String dump(@NotNull String directoryFilePath) {
        return ChronicleQueue.singleBuilder(directoryFilePath).build().dump();
    }

    private static WireOut intForBinding(ValueOut wireOut, final LongValue value) {
        return value instanceof TwoLongValue ?
                wireOut.int128forBinding(0L, 0L, (TwoLongValue) value) :
                wireOut.int64forBinding(0L, value);

    }

    private LongValue loadWritePosition(@NotNull WireIn wire) {

        final ValueIn read = wire.read(MetaDataField.writePosition);

        final int code;
        final long start = wire.bytes().readPosition();

        try {
            wire.consumePadding();
            code = wire.bytes().uncheckedReadUnsignedByte();
        } finally {
            wire.bytes().readPosition(start);
        }

        if (code == BinaryWireCode.I64_ARRAY) {
            TwoLongValue result = wire.newTwoLongReference();
            // when the write position is and array it also encodes the sequence number in the write position as the second long value
            read.int128(result);
            return result;
        }

        final LongValue result = wire.newLongReference();
        read.int64(result);
        return result;

    }

    @NotNull
    @Override
    public File file() {
        return mappedFile.file();
    }

    @NotNull
    @Override
    public String dump() {
        return dump(false);
    }

    @NotNull
    @Override
    public String shortDump() {
        return dump(true);
    }

    private String dump(boolean abbrev) {
        MappedBytes bytes = MappedBytes.mappedBytes(mappedFile);
        try {
            bytes.readLimit(bytes.realCapacity());
            final Wire w = WireType.BINARY.apply(bytes);
            if (dataVersion > 0)
                w.usePadding(true);
            return Wires.fromSizePrefixedBlobs(w, abbrev);
        } finally {
            bytes.release();
        }
    }

    @Override
    public String dumpHeader() {
        MappedBytes bytes = MappedBytes.mappedBytes(mappedFile);
        try {
            int size = bytes.readInt(0);
            if (!Wires.isReady(size))
                return "not ready";
            bytes.readLimit(Wires.lengthOf(size) + 4);
            return Wires.fromSizePrefixedBlobs(bytes);
        } finally {
            bytes.release();
        }
    }

    @Override
    public long writePosition() {
        return this.writePosition.getVolatileValue();
    }

    @NotNull
    @Override
    public WireStore writePosition(long position) {
        assert singleThreadedAccess();
        assert writePosition.getVolatileValue() + mappedFile.chunkSize() > position;
        assert Wires.isReadyData(mappedBytes.readVolatileInt(position));
        writePosition.setMaxValue(position);
        return this;
    }

    /**
     * Moves the position to the index
     *
     * @param ec    the data structure we are navigating
     * @param index the index we wish to move to
     * @return whether the index was found for reading.
     */
    @Nullable
    @Override
    public ScanResult moveToIndexForRead(@NotNull ExcerptContext ec, long index) {
        try {
            return indexing.moveToIndex(ec, index);
        } catch (@NotNull UnrecoverableTimeoutException e) {
            return ScanResult.NOT_REACHED;
        }
    }

    @Override
    public long moveToEndForRead(@NotNull Wire w) {
        return indexing.moveToEnd(w);
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
        return this.refCount.tryReserve();
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

    @Override
    public long sequenceForPosition(@NotNull final ExcerptContext ec, final long position, boolean inclusive) throws
            UnrecoverableTimeoutException, StreamCorruptedException {
        return indexing.sequenceForPosition(ec, position, inclusive);
    }

    @Override
    public long lastSequenceNumber(@NotNull ExcerptContext ec) throws StreamCorruptedException {
        return indexing.lastSequenceNumber(ec);
    }

    @NotNull
    @Override
    public String toString() {
        return "SingleChronicleQueueStore{" +
                "indexing=" + indexing +
                ", writePosition/seq=" + writePosition.toString() +
                ", mappedFile=" + mappedFile +
                ", refCount=" + refCount +
                '}';
    }

    // *************************************************************************
    // Marshalling
    // *************************************************************************

    private void onCleanup() {
        Closeable.closeQuietly(writePosition);
        Closeable.closeQuietly(indexing);
        mappedBytes.release();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        ValueOut wireOut = wire.write(MetaDataField.writePosition);
        intForBinding(wireOut, writePosition)
                .write(MetaDataField.indexing).typedMarshallable(this.indexing)
                .write(MetaDataField.dataFormat).int32(dataVersion);
    }

    @Override
    public void initIndex(@NotNull Wire wire) {
        try {
            indexing.initIndex(wire);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public boolean indexable(long index) {
        return indexing.indexable(index);
    }

    @Override
    public void setPositionForSequenceNumber(@NotNull final ExcerptContext ec, long sequenceNumber,
                                             long position)
            throws UnrecoverableTimeoutException, StreamCorruptedException {

        sequence.setSequence(sequenceNumber, position);

        long nextSequence = indexing.nextEntryToBeIndexed();
        if (nextSequence > sequenceNumber)
            return;

        indexing.setPositionForSequenceNumber(ec, sequenceNumber, position);

    }

    @Override
    public ScanResult linearScanTo(final long index, final long knownIndex, final ExcerptContext ec, final long knownAddress) {
        return indexing.linearScanTo(index, knownIndex, ec, knownAddress);
    }

    @Override
    public boolean writeEOF(@NotNull Wire wire, long timeoutMS) {
        String fileName = mappedFile.file().getAbsolutePath();

        // just in case we are about to release this
        if (wire.bytes().tryReserve()) {
            try {
                return writeEOFAndShrink(wire, timeoutMS);

            } finally {
                wire.bytes().release();
            }
        }

        try (MappedBytes bytes = MappedBytes.mappedBytes(mappedFile.file(), mappedFile.chunkSize())) {
            Wire wire0 = WireType.valueOf(wire).apply(bytes);
            return writeEOFAndShrink(wire0, timeoutMS);

        } catch (Exception e) {
            Jvm.warn().on(getClass(), "unable to write the EOF file=" + fileName, e);
            return false;
        }
    }

    boolean writeEOFAndShrink(@NotNull Wire wire, long timeoutMS) {
        if (wire.writeEndOfWire(timeoutMS, TimeUnit.MILLISECONDS, writePosition())) {
            // only if we just written EOF
            QueueFileShrinkManager.scheduleShrinking(mappedFile.file(), wire.bytes().writePosition());
            if (Jvm.isDebug())
                Jvm.debug().on(getClass(), "Shrunk file " + mappedFile.file());
            return true;
        }
        return false;
    }

    @Override
    public int dataVersion() {
        return dataVersion;
    }

    int rollIndexCount() {
        return indexing.indexCount();
    }

    int rollIndexSpacing() {
        return indexing.indexSpacing();
    }

    private synchronized boolean singleThreadedAccess() {
        if (lastAccessedThread == null) {
            lastAccessedThread = Thread.currentThread();
        }
        return lastAccessedThread == Thread.currentThread();
    }
}

