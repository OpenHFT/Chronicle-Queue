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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static java.lang.ThreadLocal.withInitial;
import static net.openhft.chronicle.wire.Wires.NOT_INITIALIZED;

public class SingleChronicleQueueStore implements WireStore {
    private static final long LONG_NOT_READY = -1;
    private static final long NUMBER_OF_ENTRIES_IN_EACH_INDEX = 1 << 17;

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Indexing.class, "Indexing");
        ClassAliasPool.CLASS_ALIASES.addAlias(Roll.class, "Roll");
    }

    @NotNull
    private final WireType wireType;
    @NotNull
    private final Roll roll;
    @NotNull
    private final LongValue writePosition;
    private final MappedFile mappedFile;
    @NotNull
    private final Indexing indexing;
    @Nullable
    private Closeable resourceCleaner;
    private final ReferenceCounter refCount = ReferenceCounter.onReleased(this::performRelease);

    /**
     * used by {@link net.openhft.chronicle.wire.Demarshallable}
     *
     * @param wire a wire
     */
    @UsedViaReflection
    private SingleChronicleQueueStore(WireIn wire) {
        wireType = wire.read(MetaDataField.wireType).object(WireType.class);
        assert wireType != null;

        this.writePosition = wire.newLongReference();
        wire.read(MetaDataField.writePosition).int64(writePosition);

        this.roll = wire.read(MetaDataField.roll).typedMarshallable();

        @NotNull final MappedBytes mappedBytes = (MappedBytes) (wire.bytes());
        this.mappedFile = mappedBytes.mappedFile();
        this.indexing = wire.read(MetaDataField.indexing).typedMarshallable();
        assert indexing != null;
        indexing.writePosition = writePosition;
    }

    /**
     * @param rollCycle    the current rollCycle
     * @param wireType     the wire type that is being used
     * @param mappedBytes  used to mapped the data store file
     * @param rollEpoc     sets an epoch offset as the number of number of milliseconds since
     * @param indexCount   the number of entries in each index.
     * @param indexSpacing the spacing between indexed entries.
     */
    public SingleChronicleQueueStore(@Nullable RollCycle rollCycle,
                                     @NotNull final WireType wireType,
                                     @NotNull MappedBytes mappedBytes,
                                     long rollEpoc, int indexCount, int indexSpacing) {
        this.roll = new Roll(rollCycle, rollEpoc);
        this.resourceCleaner = null;
        this.wireType = wireType;
        this.mappedFile = mappedBytes.mappedFile();
        indexCount = Maths.nextPower2(indexCount, 8);
        indexSpacing = Maths.nextPower2(indexSpacing, 1);
        this.indexing = new Indexing(wireType, indexCount, indexSpacing);
        indexing.writePosition =
                this.writePosition = wireType.newLongReference().get();
    }

    public static void dumpStore(Wire wire) {
        Bytes<?> bytes = wire.bytes();
        bytes.readPosition(0);
        System.out.println(Wires.fromSizePrefixedBlobs(bytes));
    }

    @Override
    public String dump() {
        MappedBytes bytes = MappedBytes.mappedBytes(mappedFile);
        bytes.readLimit(bytes.realCapacity());
        return Wires.fromSizePrefixedBlobs(bytes);
    }

    @Override
    public long writePosition() {
        return this.writePosition.getVolatileValue();
    }

    @Override
    public WireStore writePosition(long position) {
        writePosition.setMaxValue(position);
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
     * @return the last index available on the file system
     */
    @Override
    public long lastEntryIndexed(Wire wire, long timeoutMS) {
        return this.indexing.lastEntryIndexed(wire, timeoutMS);
    }

    @Override
    public boolean appendRollMeta(@NotNull Wire wire, long cycle, long timeoutMS) throws TimeoutException {
        wire.writeEndOfWire(timeoutMS, TimeUnit.MILLISECONDS);
        return true;
    }

    /**
     * Moves the position to the index
     *
     * @param wire      the data structure we are navigating
     * @param index     the index we wish to move to
     * @param timeoutMS
     * @return whether the index was found for reading.
     */
    @Override
    public ScanResult moveToIndex(@NotNull Wire wire, long index, long timeoutMS) throws TimeoutException {
        return indexing.moveToIndex(wire, index, timeoutMS);
    }

    @Override
    public void reserve() throws IllegalStateException {
        this.refCount.reserve();
    }

    @Override
    public void release() throws IllegalStateException {
        this.refCount.release();
    }

    // *************************************************************************
    // BOOTSTRAP
    // *************************************************************************

    @Override
    public long refCount() {
        return this.refCount.get();
    }

    /**
     * @return creates a new instance of mapped bytes, because, for example the tailer and appender
     * can be at different locations.
     */
    @NotNull
    @Override
    public MappedBytes mappedBytes() {
        return MappedBytes.mappedBytes(mappedFile);
    }

    // *************************************************************************
    // Utilities
    // *************************************************************************

    @Override
    public long indexForPosition(Wire wire, long position, long timeoutMS) throws EOFException, TimeoutException {
        return indexing.indexForPosition(wire, position, timeoutMS);
    }

    // *************************************************************************
    // MARSHALLABLE
    // *************************************************************************

    private synchronized void performRelease() {
        //TODO: implement
        try {
            if (this.resourceCleaner != null) {
                this.resourceCleaner.close();
            }
        } catch (IOException e) {
            //TODO
        }
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(MetaDataField.wireType).object(wireType)
                .write(MetaDataField.writePosition).int64forBinding(0L, writePosition)
                .write(MetaDataField.roll).typedMarshallable(this.roll)
                .write(MetaDataField.indexing).typedMarshallable(this.indexing);
    }

    // *************************************************************************
    // INDEXING
    // *************************************************************************

    enum MetaDataField implements WireKey {
        wireType,
        writePosition,
        roll,
        indexing;

        @Nullable
        @Override
        public Object defaultValue() {
            throw new IORuntimeException("field " + name() + " required");
        }
    }

    enum IndexingFields implements WireKey {
        indexCount, indexSpacing, index2Index, lastIndex
    }

    enum RollFields implements WireKey {
        cycle, length, format, epoch,
    }

    // *************************************************************************
    // ROLLING
    // *************************************************************************

    static class Indexing implements Demarshallable, WriteMarshallable {
        private final int indexCount, indexCountBits;
        private final int indexSpacing, indexSpacingBits;
        private final LongValue index2Index;
        private final LongValue nextEntryToIndex;
        private final ThreadLocal<LongArrayValues> index2indexArray;
        private final ThreadLocal<LongArrayValues> indexArray;
        private final WriteMarshallable index2IndexTemplate;
        private final WriteMarshallable indexTemplate;
        LongValue writePosition;

        /**
         * used by {@link net.openhft.chronicle.wire.Demarshallable}
         *
         * @param wire a wire
         */
        @UsedViaReflection
        private Indexing(@NotNull WireIn wire) {
            this(wire.read(IndexingFields.indexCount).int32(),
                    wire.read(IndexingFields.indexSpacing).int32(),
                    wire.read(IndexingFields.index2Index).int64ForBinding(wire.newLongReference()),
                    wire.read(IndexingFields.lastIndex).int64ForBinding(wire.newLongReference()),
                    wire::newLongArrayReference);
        }

        Indexing(@NotNull WireType wireType, int indexCount, int indexSpacing) {
            this(indexCount, indexSpacing, wireType.newLongReference().get(), wireType.newLongReference().get(), wireType.newLongArrayReference());
        }

        public Indexing(int indexCount, int indexSpacing, LongValue index2Index, LongValue nextEntryToIndex, Supplier<LongArrayValues> longArraySupplier) {
            this.indexCount = indexCount;
            indexCountBits = Maths.intLog2(indexCount);
            this.indexSpacing = indexSpacing;
            indexSpacingBits = Maths.intLog2(indexSpacing);
            this.index2Index = index2Index;
            this.nextEntryToIndex = nextEntryToIndex;
            this.index2indexArray = withInitial(longArraySupplier);
            this.indexArray = withInitial(longArraySupplier);
            index2IndexTemplate = w -> w.writeEventName(() -> "index2index")
                    .int64array(indexCount);
            indexTemplate = w -> w.writeEventName(() -> "index")
                    .int64array(indexCount);
        }

        public long toAddress0(long index) {
            long siftedIndex = index >> (indexSpacingBits + indexCountBits);
            long mask = indexCount - 1L;
            long maskedShiftedIndex = mask & siftedIndex;
            // convert to an offset
            return maskedShiftedIndex;
        }

        public long toAddress1(long index) {
            long siftedIndex = index >> indexSpacingBits;
            long mask = indexCount - 1L;
            // convert to an offset
            return mask & siftedIndex;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(IndexingFields.indexCount).int64(indexCount)
                    .write(IndexingFields.indexSpacing).int64(indexSpacing)
                    .write(IndexingFields.index2Index).int64forBinding(0L, index2Index)
                    .write(IndexingFields.lastIndex).int64forBinding(0L, nextEntryToIndex);
        }

        /**
         * atomically gets or creates the address of the first index the index is create and another
         * except into the queue, however this except is treated as meta data and does not increment
         * the last index, in otherword it is not possible to access this except by calling index(),
         * it effectively invisible to the end-user
         *
         * @param wire the current wire
         * @return the position of the index
         */
        long indexToIndex(@NotNull final Wire wire, long timeoutMS) throws EOFException, TimeoutException {
            long index2Index = this.index2Index.getVolatileValue();
            return index2Index > 0 ? index2Index : acquireIndex2Index(wire, timeoutMS);
        }

        long acquireIndex2Index(Wire wire, long timeoutMS) throws EOFException, TimeoutException {
            long start = System.currentTimeMillis();
            try {
                while (System.currentTimeMillis() < start + timeoutMS) {
                    long index2Index = this.index2Index.getVolatileValue();

                    if (index2Index == LONG_NOT_READY) {
                        wire.pauser().pause(timeoutMS, TimeUnit.MILLISECONDS);
                        continue;
                    }

                    if (index2Index != NOT_INITIALIZED)
                        return index2Index;

                    if (!this.index2Index.compareAndSwapValue(NOT_INITIALIZED, LONG_NOT_READY))
                        continue;
                    long index = NOT_INITIALIZED;
                    try {
                        index = newIndex(wire, true, timeoutMS);
                    } finally {
                        this.index2Index.setOrderedValue(index);
                    }
                    return index;
                }
            } finally {
                wire.pauser().reset();
            }
            throw new IllegalStateException("index2index NOT_READY for too long.");
        }

        @NotNull
        private LongArrayValues array(@NotNull WireIn w, @NotNull LongArrayValues using, boolean index2index) {
            final StringBuilder sb = Wires.acquireStringBuilder();
            @NotNull final ValueIn valueIn = w.readEventName(sb);
            String name = index2index ? "index2index" : "index";
            if (!name.contentEquals(sb))
                throw new IllegalStateException("expecting index, was " + sb);

            valueIn.int64array(using, this, (o1, o2) -> {
            });
            return using;
        }

        /**
         * Creates a new Excerpt containing and index which will be 1L << 17L bytes long, This
         * method is used for creating both the primary and secondary indexes. Chronicle Queue uses
         * a root primary index ( each entry in the primary index points to a unique a secondary
         * index. The secondary index only records the address of every 64th except, the except are
         * linearly scanned from there on.  )
         *
         * @param wire the current wire
         * @return the address of the Excerpt containing the usable index, just after the header
         */
        long newIndex(@NotNull Wire wire, boolean index2index, long timeoutMS) throws EOFException {
            long writePosition = this.writePosition.getValue();
            wire.bytes().writePosition(writePosition);
            long position = 0;
            try {
                position = wire.writeHeader(timeoutMS, TimeUnit.MILLISECONDS);
                WriteMarshallable writer = index2index ? index2IndexTemplate : indexTemplate;
                writer.writeMarshallable(wire);
                wire.updateHeader(position, true);

            } catch (TimeoutException | StreamCorruptedException e) {
                throw new AssertionError(e);
            }

            this.writePosition.setMaxValue(wire.bytes().writePosition());
            return position;
        }

        long newIndex(Wire wire, LongArrayValues index2Index, long index2, long timeoutMS) throws EOFException {
            if (index2Index.compareAndSet(index2, NOT_INITIALIZED, LONG_NOT_READY)) {
                long pos = newIndex(wire, false, timeoutMS);
                if (index2Index.compareAndSet(index2, LONG_NOT_READY, pos)) {
                    index2Index.setMaxUsed(index2 + 1);
                    return pos;
                }
                throw new IllegalStateException("Index " + index2 + " in index2index was altered");
            }
            for (; ; ) {
                long pos = index2Index.getVolatileValueAt(index2);
                if (pos == LONG_NOT_READY) {
                    wire.pauser().pause();
                } else {
                    wire.pauser().reset();
                    return pos;
                }
            }
        }

        /**
         * Moves the position to the {@code index} <p> The indexes are stored in many excerpts, so
         * the index2index tells chronicle where ( in other words the address of where ) the root
         * first level targetIndex is stored. The indexing works like a tree, but only 2 levels
         * deep, the root of the tree is at index2index ( this first level targetIndex is 1MB in
         * size and there is only one of them, it only holds the addresses of the second level
         * indexes, there will be many second level indexes ( created on demand ), each is about 1MB
         * in size  (this second level targetIndex only stores the position of every 64th excerpt),
         * so from every 64th excerpt a linear scan occurs.
         *
         * @param wire  the data structure we are navigating
         * @param index the index we wish to move to
         * @return the position of the {@code targetIndex} or -1 if the index can not be found
         */
        ScanResult moveToIndex(@NotNull final Wire wire, final long index, long timeoutMS) throws TimeoutException {
            try {
                ScanResult scanResult = moveToIndex0(wire, index, timeoutMS);
                if (scanResult != null)
                    return scanResult;
            } catch (EOFException e) {
                // scan from the start.
            }
            return moveToIndexFromTheStart(wire, index);
        }

        private ScanResult moveToIndexFromTheStart(@NotNull Wire wire, long index) {
            try {
                wire.bytes().readPosition(0);
                if (wire.readDataHeader())
                    return linearScan(wire, index, 0, wire.bytes().readPosition());
            } catch (EOFException e) {
            }
            return ScanResult.NOT_FOUND;
        }

        ScanResult moveToIndex0(@NotNull final Wire wire, final long index, long timeoutMS) throws EOFException, TimeoutException {

            LongArrayValues index2index = getIndex2index(wire, timeoutMS);

            @NotNull final Bytes<?> bytes = wire.bytes();
            bytes.writeLimit(bytes.capacity()).readLimit(bytes.capacity());

            long primaryOffset = toAddress0(index);

            long secondaryAddress = 0;
            long startIndex = index & ~(indexSpacing - 1);
            while (primaryOffset >= 0) {
                secondaryAddress = index2index.getValueAt(primaryOffset);
                if (secondaryAddress == 0) {
                    startIndex -= indexCount * indexSpacing;
                    primaryOffset--;
                } else {
                    break;
                }
            }

            if (secondaryAddress <= 0) {
                return null;
            }
            bytes.readPosition(secondaryAddress);
            wire.readMetaDataHeader();

            final LongArrayValues array = this.indexArray.get();

            @NotNull final LongArrayValues array1 = array(wire, array, false);
            long secondaryOffset = toAddress1(index);

            do {
                long fromAddress = array1.getValueAt(secondaryOffset);
                if (fromAddress == 0) {
                    secondaryOffset--;
                    startIndex -= indexSpacing;
                    continue;
                }

                if (index == startIndex) {
                    bytes.readLimit(bytes.capacity()).readPosition(fromAddress);
                    return ScanResult.FOUND;
                } else {
                    return linearScan(wire, index, startIndex, fromAddress);
                }

            } while (secondaryOffset >= 0);
            return null; // no index,
        }

        /**
         * moves the context to the index of {@code toIndex} by doing a linear scans form a {@code
         * fromKnownIndex} at  {@code knownAddress} <p/> note meta data is skipped and does not
         * count to the indexes
         *
         * @param wire           if successful, moves the context to an address relating to the
         *                       index {@code toIndex }
         * @param toIndex        the index that we wish to move the context to
         * @param fromKnownIndex a know index ( used as a starting point )
         * @param knownAddress   a know address ( used as a starting point )
         * @see net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.Indexing#moveToIndex
         */

        private ScanResult linearScan(@NotNull final Wire wire,
                                      final long toIndex,
                                      final long fromKnownIndex,
                                      final long knownAddress) {
            @NotNull
            final Bytes<?> bytes = wire.bytes();

            long end = writePosition.getValue();
            bytes.readLimit(bytes.capacity()).readPosition(knownAddress);

            for (long i = fromKnownIndex; ; i++) {
                try {
                    if (wire.readDataHeader()) {
                        if (i == toIndex)
                            return ScanResult.FOUND;
                        if (bytes.readPosition() > end)
                            return ScanResult.NOT_REACHED;
                        bytes.readSkip(Wires.lengthOf(bytes.readInt()));
                        continue;
                    }
                } catch (EOFException e) {
                    // reached the end of the file.
                }
                return i == toIndex ? ScanResult.NOT_FOUND : ScanResult.NOT_REACHED;
            }
        }

        private long linearScanByPosition(@NotNull final Wire wire,
                                          final long toPosition,
                                          final long fromKnownIndex,
                                          final long knownAddress) throws EOFException {
            @NotNull
            final Bytes<?> bytes = wire.bytes();

            bytes.readLimit(writePosition.getValue()).readPosition(knownAddress);

            for (long i = fromKnownIndex; ; i++) {

                boolean found = wire.readDataHeader();

                if (bytes.readPosition() == toPosition)
                    return i;

                bytes.readSkip(Wires.lengthOf(bytes.readInt()));
                if (bytes.readPosition() > toPosition)
                    return i;

                if (!found) {
                    if (toPosition == Long.MAX_VALUE)
                        return i - 1;
                    throw new EOFException();
                }
            }
        }

        public long lastEntryIndexed(Wire wire, long timeoutMS) {
            try {
                indexForPosition(wire, Long.MAX_VALUE, timeoutMS);
            } catch (Exception e) {
                // ignore.
            }

            return nextEntryToIndex.getValue() - 1;
        }

        public LongArrayValues getIndex2index(Wire wire, long timeoutMS) throws EOFException, TimeoutException {
            LongArrayValues values = index2indexArray.get();
            if (((Byteable) values).bytesStore() != null)
                return values;
            final long indexToIndex0 = indexToIndex(wire, timeoutMS);
            wire.bytes().readLimit(wire.bytes().capacity());
            try (DocumentContext context = wire.readingDocument(indexToIndex0)) {
                if (!context.isPresent() || !context.isMetaData()) {
                    dumpStore(wire);
                    throw new IllegalStateException("document present=" + context.isPresent() + ", metaData=" + context.isMetaData());
                }

                return array(wire, values, true);
            }
        }

        public long indexForPosition(Wire wire, long position, long timeoutMS) throws EOFException, TimeoutException {
            // find the index2index
            final LongArrayValues index2indexArr = getIndex2index(wire, timeoutMS);
            final LongArrayValues indexArr = indexArray.get();
            long lastKnownAddress = 0;
            long lastKnownIndex = -1;
            Bytes<?> bytes = wire.bytes();
            for (int index2 = 0; index2 < indexCount; index2++) {
                long secondaryAddress = index2indexArr.getValueAt(index2);
                if (secondaryAddress == 0)
                    secondaryAddress = newIndex(wire, index2indexArr, index2, timeoutMS);

                bytes.readLimit(bytes.capacity());
                try (DocumentContext context = wire.readingDocument(secondaryAddress)) {
                    if (!context.isPresent() || !context.isMetaData())
                        throw new IllegalStateException("document present=" + context.isPresent() + ", metaData=" + context.isMetaData());

                    @NotNull final LongArrayValues array1 = array(wire, indexArr, false);
                    // check the last one first.
                    long posN = array1.getValueAt(indexCount - 1);
                    if (posN > 0 && posN < position) {
                        lastKnownAddress = posN;
                        lastKnownIndex = ((index2 + 1L << indexCountBits) - 1) << indexSpacingBits;
                        continue;
                    }

                    // otherwise we need to scan the current entries.
                    for (int index1 = 0; index1 < indexCount; index1++) {
                        long pos = array1.getValueAt(index1);
                        if (pos != 0) {
                            lastKnownAddress = pos;
                            lastKnownIndex = ((long) index2 << (indexCountBits + indexSpacingBits)) + (index1 << indexSpacingBits);
                            continue;
                        }
                        ScanResult scanResult;
                        long nextIndex;
                        if (lastKnownIndex < 0) {
                            scanResult = firstScan(wire);
                            nextIndex = 0;
                        } else {
                            nextIndex = lastKnownIndex + indexSpacing;
                            scanResult = linearScan(wire, nextIndex, lastKnownIndex, lastKnownAddress);
                        }
                        if (scanResult == ScanResult.FOUND) {
                            long nextPosition = bytes.readPosition();
                            array1.setOrderedValueAt(index1, lastKnownAddress = nextPosition);
                            array1.setMaxUsed(index1 + 1);

                            if (nextPosition == position) {
                                nextEntryToIndex.setMaxValue(nextIndex + 1);
                                return nextIndex;
                            }
                            lastKnownIndex = nextIndex;
                        } else {
                            long ret = linearScanByPosition(wire, position, lastKnownIndex, lastKnownAddress);
                            nextEntryToIndex.setMaxValue(ret + 1);
                            return ret;
                        }
                    }
                }
            }
            throw new AssertionError();
        }

        @NotNull
        private ScanResult firstScan(Wire wire) {
            try {
                wire.bytes().readPosition(0);
                return wire.readDataHeader() ? ScanResult.FOUND : ScanResult.NOT_REACHED;
            } catch (EOFException e) {
                return ScanResult.NOT_FOUND;
            }
        }
    }

    static class Roll implements Demarshallable, WriteMarshallable {
        private final long epoch;
        private final int length;
        @Nullable
        private final String format;

        /**
         * used by {@link net.openhft.chronicle.wire.Demarshallable}
         *
         * @param wire a wire
         */
        @UsedViaReflection
        private Roll(WireIn wire) {
            length = wire.read(RollFields.length).int32();
            format = wire.read(RollFields.format).text();
            epoch = wire.read(RollFields.epoch).int64();
        }

        Roll(@NotNull RollCycle rollCycle, long rollEpoch) {
            this.length = rollCycle.length();
            this.format = rollCycle.format();
            this.epoch = rollEpoch;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(RollFields.length).int32(length)
                    .write(RollFields.format).text(format)
                    .write(RollFields.epoch).int64(epoch);
        }

        /**
         * @return an epoch offset as the number of number of milliseconds since January 1, 1970,
         * 00:00:00 GMT
         */
        public long epoch() {
            return this.epoch;
        }
    }
}

