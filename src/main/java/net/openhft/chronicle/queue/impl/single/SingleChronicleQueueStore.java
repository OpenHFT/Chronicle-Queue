/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
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

import java.io.*;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class SingleChronicleQueueStore extends AbstractCloseable implements WireStore {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(SCQIndexing.class);
    }

    @NotNull
    final SCQIndexing indexing;
    // retains the MappedBytes used by the MappedFile
    @NotNull
    private final LongValue writePosition;
    @NotNull
    private final MappedBytes mappedBytes;
    @NotNull
    private final MappedFile mappedFile;
    private final int dataVersion;
    @NotNull
    private final transient Sequence sequence;

    private int cycle;

    /**
     * used by {@link net.openhft.chronicle.wire.Demarshallable}
     *
     * @param wire a wire
     */
    @UsedViaReflection
    private SingleChronicleQueueStore(@NotNull WireIn wire) {
        boolean failed = true;
        assert wire.startUse();
        try {
            writePosition = loadWritePosition(wire);
            this.mappedBytes = (MappedBytes) wire.bytes();
            this.mappedFile = mappedBytes.mappedFile();
            mappedFile.reserve(this);
            this.indexing = Objects.requireNonNull(wire.read(MetaDataField.indexing).typedMarshallable());
            this.indexing.writePosition = writePosition;
            this.sequence = new RollCycleEncodeSequence(writePosition, rollIndexCount(), rollIndexSpacing());
            this.indexing.sequence = sequence;
            if (wire.bytes().readRemaining() > 0) {
                final int version = wire.read(MetaDataField.dataFormat).int32();
                this.dataVersion = version > 1 ? 0 : version;
            } else
                this.dataVersion = 0;

            disableThreadSafetyCheck(true);
            failed = false;
        } finally {
            if (failed)
                close();
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
        mappedFile.reserve(this);
        indexCount = Maths.nextPower2(indexCount, 8);
        indexSpacing = Maths.nextPower2(indexSpacing, 1);

        this.indexing = new SCQIndexing(wireType, indexCount, indexSpacing);
        this.indexing.writePosition = this.writePosition = wireType.newTwoLongReference().get();
        this.indexing.sequence = this.sequence = new RollCycleEncodeSequence(writePosition,
                rollCycle.defaultIndexCount(),
                rollCycle.defaultIndexSpacing());
        this.dataVersion = 1;

        disableThreadSafetyCheck(true);
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
        try (MappedBytes bytes = MappedBytes.mappedBytes(mappedFile)) {
            bytes.readLimit(bytes.realCapacity());
            final Wire w = WireType.BINARY.apply(bytes);
            w.usePadding(dataVersion > 0);
            return Wires.fromSizePrefixedBlobs(w, abbrev);
        }
    }

    @Override
    public String dumpHeader() {
        try (MappedBytes bytes = MappedBytes.mappedBytes(mappedFile)) {
            int size = bytes.readInt(0);
            if (!Wires.isReady(size))
                return "not ready";
            bytes.readLimit(Wires.lengthOf(size) + 4L);
            return Wires.fromSizePrefixedBlobs(bytes);
        }
    }

    @Override
    public long writePosition() {
        return this.writePosition.getVolatileValue();
    }

    @NotNull
    @Override
    public WireStore writePosition(long position) {
        throwExceptionIfClosed();

        assert writePosition.getVolatileValue() + mappedFile.chunkSize() > position;
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
        throwExceptionIfClosed();

        try {
            return indexing.moveToIndex(ec, index);
        } catch (@NotNull UnrecoverableTimeoutException e) {
            return ScanResult.NOT_REACHED;
        }
    }

    /**
     * Moves the position to the start
     *
     * @param ec the data structure we are navigating
     * @return whether the index was found for reading.
     */
    @Nullable
    @Override
    public ScanResult moveToStartForRead(@NotNull ExcerptContext ec) {
        throwExceptionIfClosed();

        Wire wire = ec.wire();
        wire.bytes().readPositionUnlimited(0);

        try {
            final WireIn.HeaderType headerType = wire.readDataHeader(true);
            switch (headerType) {
                case DATA:
                case META_DATA:
                    return ScanResult.FOUND;
                case NONE:
                    return ScanResult.NOT_REACHED;
                case EOF:
                    return ScanResult.END_OF_FILE;
                default:
                    throw new AssertionError("headerType=" + headerType);
            }
        } catch (EOFException eof) {
            return ScanResult.END_OF_FILE;
        }
    }

    @Override
    public long moveToEndForRead(@NotNull Wire w) {
        throwExceptionIfClosed();

        return indexing.moveToEnd(w);
    }

    @Override
    protected void performClose() {
        Closeable.closeQuietly(writePosition);
        Closeable.closeQuietly(indexing);

        mappedBytes.release(INIT);
        try {
            mappedFile.release(this);
        } catch (IllegalStateException e) {
            Jvm.warn().on(getClass(), "trouble releasing " + mappedFile, e);
        }
    }

    /**
     * @return creates a new instance of mapped bytes, because, for example the tailer and appender
     * can be at different locations.
     */
    @NotNull
    @Override
    public MappedBytes bytes() {
        throwExceptionIfClosed();

        return MappedBytes.mappedBytes(mappedFile)
                .disableThreadSafetyCheck(true);
    }

    @Override
    public long sequenceForPosition(@NotNull final ExcerptContext ec, final long position, boolean inclusive) throws StreamCorruptedException {
        throwExceptionIfClosed();

        return indexing.sequenceForPosition(ec, position, inclusive);
    }

    @Override
    @Deprecated
    public long lastSequenceNumber(@NotNull ExcerptContext ec) throws StreamCorruptedException {
        return approximateLastSequenceNumber(ec);
    }

    public long approximateLastSequenceNumber(@NotNull ExcerptContext ec) throws StreamCorruptedException {
        throwExceptionIfClosedInSetter();
        return indexing.lastSequenceNumber(ec, true);
    }

    public long exactLastSequenceNumber(@NotNull ExcerptContext ec) throws StreamCorruptedException {
        throwExceptionIfClosedInSetter();
        return indexing.lastSequenceNumber(ec, false);
    }

    @NotNull
    @Override
    public String toString() {
        return "SingleChronicleQueueStore{" +
                "indexing=" + indexing +
                ", writePosition/seq=" + writePosition +
                ", mappedFile=" + mappedFile +
                ", isClosed=" + isClosed() +
                '}';
    }

    // *************************************************************************
    // Marshalling
    // *************************************************************************

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {

        ValueOut wireOut = wire.write(MetaDataField.writePosition);
        intForBinding(wireOut, writePosition)
                .write(MetaDataField.indexing).typedMarshallable(this.indexing)
                .write(MetaDataField.dataFormat).int32(dataVersion);
    }

    @Override
    public void initIndex(@NotNull Wire wire) {
        throwExceptionIfClosedInSetter();

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
    public void setPositionForSequenceNumber(@NotNull final ExcerptContext ec,
                                             long sequenceNumber,
                                             long position) throws StreamCorruptedException {
        throwExceptionIfClosedInSetter();

        sequence.setSequence(sequenceNumber, position);

        long nextSequence = indexing.nextEntryToBeIndexed();
        if (nextSequence > sequenceNumber)
            return;

        indexing.setPositionForSequenceNumber(ec, sequenceNumber, position);

    }

    @Override
    public ScanResult linearScanTo(final long index, final long knownIndex, final ExcerptContext ec, final long knownAddress) {
        throwExceptionIfClosed();

        return indexing.linearScanTo(index, knownIndex, ec, knownAddress);
    }

    @Override
    public boolean writeEOF(@NotNull Wire wire, long timeoutMS) {
        throwExceptionIfClosed();

        String fileName = mappedFile.file().getAbsolutePath();

        // just in case we are about to release this
        if (wire.bytes().tryReserve(this)) {
            try {
                return writeEOFAndShrink(wire, timeoutMS);

            } finally {
                wire.bytes().release(this);
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

    public SingleChronicleQueueStore cycle(int cycle) {
        throwExceptionIfClosedInSetter();

        this.cycle = cycle;
        return this;
    }

    public int cycle() {
        return cycle;
    }

    public File currentFile() {
        return mappedFile.file();
    }
}

