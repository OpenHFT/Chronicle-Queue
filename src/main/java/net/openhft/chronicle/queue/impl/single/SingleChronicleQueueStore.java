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
import java.io.IOException;
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
//    @UsedViaReflection
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
    SingleChronicleQueueStore(@Nullable RollCycle rollCycle,
                              @NotNull final WireType wireType,
                              @NotNull MappedBytes mappedBytes,
                              long rollEpoc, int indexCount, int indexSpacing) {
        this.roll = new Roll(rollCycle, rollEpoc, wireType);
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
        MappedBytes bytes = new MappedBytes(mappedFile);
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
    public long lastEntryIndexed(Wire wire) {
        return this.indexing.lastEntryIndexed(wire);
    }

    @Override
    public boolean appendRollMeta(@NotNull Wire wire, long cycle) {
        if ((writePosition() & ROLLED_BIT) != 0)
            return false;
        wire.writeDocument(true, d -> d.write(MetaDataField.roll).int32(cycle));
        writePosition(wire.bytes().writePosition() | ROLLED_BIT);
        return true;
    }

    /**
     * Moves the position to the index
     *
     * @param wire  the data structure we are navigating
     * @param index the index we wish to move to
     * @return the position of the {@code targetIndex}  or -1 if the index can not be found
     */
    @Override
    public long moveToIndex(@NotNull Wire wire, long index) {
        return indexing.moveToIndex(wire, index);
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
        final MappedBytes mappedBytes = new MappedBytes(mappedFile);
        mappedBytes.readPosition(0);
        mappedBytes.writePosition(writePosition() & (WireStore.ROLLED_BIT - 1));
        return mappedBytes;
    }

    @Override
    public long storeIndexLocation(@NotNull Wire wire, long position) {
        return indexing.storeIndexLocation(wire, position);
    }

    // *************************************************************************
    // Utilities
    // *************************************************************************

    @Override
    public long indexForPosition(Wire wire, long position) {
        return indexing.indexForPosition(wire, position);
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
        long indexToIndex(@NotNull final Wire wire) {
            long index2Index = this.index2Index.getVolatileValue();
            return index2Index > 0 ? index2Index : acquireIndex2Index(wire);
        }

        long acquireIndex2Index(Wire wire) {
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() < start + 10e9) {
                long index2Index = this.index2Index.getVolatileValue();

                if (index2Index == LONG_NOT_READY) {
                    Thread.yield();
                    continue;
                }

                if (index2Index != NOT_INITIALIZED)
                    return index2Index;

                if (!this.index2Index.compareAndSwapValue(NOT_INITIALIZED, LONG_NOT_READY))
                    continue;
                final long index = newIndex(wire, true);
                this.index2Index.setOrderedValue(index);
                return index;
            }
            throw new IllegalStateException("index2index NOT_READY for too long.");
        }

        /**
         * records the the location of the sequenceNumber, only every 64th address is written to the
         * sequenceNumber file, the first sequenceNumber is stored at {@code index2index}
         *
         * @param wire    the context that we are referring to
         * @param address the address of the Excerpts which we are going to record
         */
        public long storeIndexLocation(@NotNull Wire wire,
                                       final long address) {

            return -1;
            /*
            lastSequenceNumber(sequenceNumber);

            long writePosition = wire.bytes().writePosition();
            try {

                if (sequenceNumber % 64 != 0)
                    return;

                final LongArrayValues array = this.longArray.get();
                final long indexToIndex0 = indexToIndex(wire);

                long secondaryAddress;
                try (DocumentContext context = wire.readingDocument(indexToIndex0)) {

                    if (!context.isPresent())
                        throw new IllegalStateException("document not found");

                    if (!context.isMetaData()) {
                        System.out.println("===\n"+Wires.fromSizePrefixedBlobs(wire.bytes(), 0, 2048)+"\n===");
//                        System.out.println("=== 495 +++\n"+Wires.fromSizePrefixedBlobs(wire.bytes(), 495, 2048)+"\n<<< 495 +++");
                        throw new IllegalStateException("sequenceNumber not found");
                    }

                    @NotNull final LongArrayValues primaryIndex = array(wire, array);
                    final long primaryOffset = toAddress0(sequenceNumber);
                    // TODO fix a race condition here.
                    secondaryAddress = primaryIndex.getValueAt(primaryOffset);

                    if (secondaryAddress == Wires.NOT_INITIALIZED) {
                        secondaryAddress = newIndex(wire);
                        writePosition = Math.max(writePosition, wire.bytes().writePosition());
                        primaryIndex.setValueAt(primaryOffset, secondaryAddress);
                    }
                }
                @NotNull final Bytes<?> bytes = wire.bytes();
                bytes.readLimit(bytes.capacity());
                try (DocumentContext context = wire.readingDocument(secondaryAddress)) {

                    @NotNull final LongArrayValues array1 = array(wire, array);
                    if (!context.isPresent())
                        throw new IllegalStateException("document not found");
                    if (!context.isMetaData())
                        throw new IllegalStateException("sequenceNumber not found");
                    array1.setValueAt(toAddress1(sequenceNumber), address);
                }

            } finally {
                wire.bytes().writePosition(writePosition);
            }
            */
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
        long newIndex(@NotNull Wire wire, boolean index2index) {
            long writePosition = this.writePosition.getValue();
            if (writePosition >= WireStore.ROLLED_BIT)
                throw new IllegalStateException("ROLLED");

            wire.bytes().writePosition(writePosition);
            long position;
            do {
                position = WireInternal.writeWireOrAdvanceIfNotEmpty(wire, true, index2index ? index2IndexTemplate : indexTemplate);
            } while (position <= 0);
            int len = Wires.lengthOf(wire.bytes().readInt(position));
            this.writePosition.setMaxValue(position + len + 4);
            return position;
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
        public long moveToIndex(@NotNull final Wire wire, final long index) {
            long ret = moveToIndex0(wire, index);
            if (ret <= 0) {
                checkIndexerUpToDate(wire);
                ret = moveToIndex0(wire, index);
            }
            return ret;
        }

        long moveToIndex0(@NotNull final Wire wire, final long index) {

            LongArrayValues index2index = getIndex2index(wire);
            final long indexToIndex0 = indexToIndex(wire);

            @NotNull final Bytes<?> bytes = wire.bytes();
            bytes.writeLimit(bytes.capacity());
            final long readPosition = bytes.readPosition();
            long limit = bytes.readLimit();
            try {
                bytes.readLimit(bytes.capacity());
                bytes.readPosition(indexToIndex0);
                long startIndex = index & ~(indexSpacing - 1);

                long primaryOffset = toAddress0(index);

                long secondaryAddress = 0;
                while (primaryOffset >= 0) {
                    secondaryAddress = index2index.getValueAt(primaryOffset);
                    if (secondaryAddress == 0) {
                        startIndex -= indexCount * indexSpacing;
                        primaryOffset--;
                    } else {
                        break;
                    }
                }
                if (secondaryAddress <= 0)
                    return -1;

                bytes.readPosition(secondaryAddress);

                try (@NotNull final DocumentContext context = wire.readingDocument()) {

                    final LongArrayValues array = this.indexArray.get();
                    if (!context.isPresent() || !context.isMetaData())
                        throw new IllegalStateException("document is present=" + context.isPresent() + ", metaData=" + context.isMetaData());

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
                            return fromAddress;
                        } else {
                            return linearScan(wire, index, startIndex, fromAddress);
                        }

                    } while (secondaryOffset >= 0);
                }
            } finally {
                bytes.readLimit(limit).readPosition(readPosition);
            }

            return -1;
        }

        /**
         * moves the context to the index of {@code toIndex} by doing a linear scans form a {@code
         * fromKnownIndex} at  {@code knownAddress} <p/> note meta data is skipped and does not
         * count to the indexes
         *
         * @param context        if successful, moves the context to an address relating to the
         *                       index {@code toIndex }
         * @param toIndex        the index that we wish to move the context to
         * @param fromKnownIndex a know index ( used as a starting point )
         * @param knownAddress   a know address ( used as a starting point )
         * @return > -1 if successful
         * @see net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.Indexing#moveToIndex
         */

        private long linearScan(@NotNull final Wire context,
                                final long toIndex,
                                final long fromKnownIndex,
                                final long knownAddress) {
            @NotNull
            final Bytes<?> bytes = context.bytes();
            final long l = bytes.readLimit();
            final long p = bytes.readPosition();

            try {
                bytes.readLimit(bytes.capacity()).readPosition(knownAddress);
                for (long i = fromKnownIndex; bytes.readRemaining() > 0; ) {

                    // wait until ready - todo add timeout
                    for (; ; ) {
                        if (Wires.isReady(bytes.readVolatileInt(bytes.readPosition()))) {
                            break;
                        } else
                            Thread.yield();
                    }

                    try (@NotNull final DocumentContext documentContext = context.readingDocument()) {

                        if (!documentContext.isPresent())
                            return -1;

                        if (!documentContext.isData())
                            continue;

                        if (toIndex == i) {
                            context.bytes().readSkip(-4);
                            return context.bytes().readPosition();
                        }
                        i++;
                    }
                }
            } finally {
                bytes.readLimit(l).readPosition(p);
            }
            return -1;
        }

        public long lastEntryIndexed(Wire wire) {
            checkIndexerUpToDate(wire);
            return nextEntryToIndex.getValue() - 1;
        }

        private long positionForIndex(Wire wire, long value) {
            return moveToIndex(wire, value);
        }

        public LongArrayValues getIndex2index(Wire wire) {
            LongArrayValues values = index2indexArray.get();
            if (((Byteable) values).bytesStore() != null)
                return values;
            final long indexToIndex0 = indexToIndex(wire);
            try (DocumentContext context = wire.readingDocument(indexToIndex0)) {
                if (!context.isPresent() || !context.isMetaData()) {
                    dumpStore(wire);
                    throw new IllegalStateException("document present=" + context.isPresent() + ", metaData=" + context.isMetaData());
                }

                return array(wire, values, true);
            }
        }

        public long indexForPosition(Wire wire, long position) {
            checkIndexerUpToDate(wire);
            // find the index2index
            final LongArrayValues index2index = getIndex2index(wire);
            int index2 = 0;

            final LongArrayValues index = indexArray.get();
            for (; index2 < indexCount; index2++) {
                long secondaryAddress = index2index.getValueAt(index2 + 1);
                if (secondaryAddress == 0)
                    break;
                try (DocumentContext context = wire.readingDocument(secondaryAddress)) {
                    if (!context.isPresent() || !context.isMetaData())
                        throw new IllegalStateException("document present=" + context.isPresent() + ", metaData=" + context.isMetaData());

                    @NotNull final LongArrayValues array1 = array(wire, index, false);
                    long position2 = array1.getValueAt(indexCount - 1);
                    if (position2 == 0 || position <= position2) {
                        index2++;
                        break;
                    }
                }
            }

            int index3 = 0;
            long address3 = index.getValueAt(0);
            for (; index3 < indexCount; index3++) {
                long address3b = index.getValueAt(index3 + 1);
                if (address3b == 0 || address3b >= position)
                    break;
                address3 = address3b;
            }
            // linear scan from here.

            int index4 = 0;
            while (address3 < position) {
                int header = wire.bytes().readInt(address3);
                if (!Wires.isReady(header))
                    throw new IllegalStateException("For an entry which is not ready at " + address3);
                if (header == 0)
                    throw new IllegalStateException("No entry at " + address3 + " " + position + " " + index4);
                if (Wires.isData(header)) {
                    index4++;
                }
                address3 += Wires.lengthOf(header) + 4;
            }
            return ((((long) index2 << indexCountBits) + index3) << indexSpacingBits) + index4;
        }

        private void checkIndexerUpToDate(Wire wire) {
            long index = nextEntryToIndex.getValue() - 1;
            long position = index < 0 ? 0L : moveToIndex0(wire, index);
            if (position < 0) {
                moveToIndex0(wire, index);
                throw new AssertionError();
            }
            LongArrayValues index2index = getIndex2index(wire);
            Bytes bytes = wire.bytes();
            long nextIndex = (index + indexSpacing) & ~(indexSpacing - 1);
            index = checkUpToDate0(wire, index, position, index2index, bytes, nextIndex);
//            System.out.println("index: " + index);
            nextEntryToIndex.setMaxValue(index + 1);
        }

        private long checkUpToDate0(Wire wire, long index, long position, LongArrayValues index2index, Bytes bytes, long nextIndex) {
            int len, header;
            // if its a known position, start with the next one.
            if (index >= 0) {
                header = bytes.readVolatileInt(position);
                len = Wires.lengthOf(header);
                position += len + 4;
            }
            for (; ; ) {
                header = bytes.readVolatileInt(position);
                len = Wires.lengthOf(header);

                if (len == 0)
                    break;
                if (Wires.isData(header))
                    index++;

                if (index < nextIndex) {
                    position += len + 4;
                    continue;
                }

                long secondaryAddress;

                final long primaryOffset = toAddress0(index);
                // initialise it if needed.
                if (index2index.compareAndSet(primaryOffset, NOT_INITIALIZED, LONG_NOT_READY)) {
                    boolean ok = false;

                    try {
                        secondaryAddress = newIndex(wire, false);
                        index2index.setValueAt(primaryOffset, secondaryAddress);
                        ok = true;
                    } finally {
                        if (!ok) {
                            System.out.println("Failed update " + primaryOffset);
                            index2index.compareAndSet(primaryOffset, LONG_NOT_READY, NOT_INITIALIZED);
                        }
                    }

                } else {
                    secondaryAddress = index2index.getValueAt(primaryOffset);
                    if (secondaryAddress == LONG_NOT_READY) {
                        long start = System.currentTimeMillis();
                        do {
                            Thread.yield();
                            secondaryAddress = index2index.getVolatileValueAt(primaryOffset);
                            if (System.currentTimeMillis() > start + 10e3)
                                throw new IllegalStateException("Index " + primaryOffset + " not ready");
                        } while (secondaryAddress == LONG_NOT_READY);
                    }
                }

                try (DocumentContext context = wire.readingDocument(secondaryAddress)) {
                    if (!context.isPresent() || !context.isMetaData())
                        throw new IllegalStateException("document present=" + context.isPresent() + ", metaData=" + context.isMetaData());

                    @NotNull final LongArrayValues indexArr = array(wire, indexArray.get(), false);
                    indexArr.setValueAt(toAddress1(index), position);
//                    assert position <= writePosition.getValue();
                    writePosition.setMaxValue(position);
                }
                nextIndex += indexSpacing;
                position += len + 4;
            }
            return index;
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
        private Roll(WireIn wire) {
            length = wire.read(RollFields.length).int32();
            format = wire.read(RollFields.format).text();

            epoch = wire.read(RollFields.epoch).int64();
        }

        Roll(@NotNull RollCycle rollCycle, long rollEpoch, WireType wireType) {
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
