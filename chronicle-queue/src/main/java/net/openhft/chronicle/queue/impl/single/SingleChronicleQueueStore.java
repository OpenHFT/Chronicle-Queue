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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.WireConstants;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.function.Function;

import static java.lang.ThreadLocal.withInitial;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.IndexOffset.toAddress0;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.IndexOffset.toAddress1;
import static net.openhft.chronicle.wire.Wires.NOT_INITIALIZED;
import static net.openhft.chronicle.wire.Wires.NOT_READY;

public class SingleChronicleQueueStore implements WireStore {

    public static final long NUMBER_OF_ENTRIES_IN_EACH_INDEX = 1 << 17;

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Bounds.class, "Bounds");
        ClassAliasPool.CLASS_ALIASES.addAlias(Indexing.class, "Indexing");
        ClassAliasPool.CLASS_ALIASES.addAlias(Roll.class, "Roll");
    }

    static Wire TEXT_TEMPLATE;
    static Wire BINARY_TEMPLATE;

    static {
        TEXT_TEMPLATE = WireType.TEXT.apply(Bytes.elasticByteBuffer());
        TEXT_TEMPLATE.writeDocument(true, w -> w.writeEventName(() -> "index")
                .int64array(NUMBER_OF_ENTRIES_IN_EACH_INDEX));

        BINARY_TEMPLATE = WireType.BINARY.apply(Bytes.elasticByteBuffer());
        BINARY_TEMPLATE.writeDocument(true, w -> w.writeEventName(() -> "index")
                .int64array(NUMBER_OF_ENTRIES_IN_EACH_INDEX));
    }

    private WireType wireType;
    private final Roll roll;
    Bounds bounds = new Bounds();
    private MappedFile mappedFile;
    private Closeable resourceCleaner;
    private final ReferenceCounter refCount = ReferenceCounter.onReleased(this::performRelease);

    private SingleChronicleQueueBuilder builder;
    private Indexing indexing;


    /**
     * Default constructor needed for self boot-strapping
     */
    SingleChronicleQueueStore() {
        this.roll = new Roll(null, 0);
    }

    /**
     * @param rollCycle
     * @param wireType
     * @param mappedBytes
     * @param rollEpoc    sets an epoch offset as the number of number of milliseconds since January
     *                    1, 1970,  00:00:00 GMT
     */
    SingleChronicleQueueStore(@Nullable RollCycle rollCycle,
                              final WireType wireType,
                              @NotNull MappedBytes mappedBytes,
                              long rollEpoc) {
        this.roll = new Roll(rollCycle, rollEpoc);
        this.resourceCleaner = null;
        this.builder = null;
        this.wireType = wireType;
        this.mappedFile = mappedBytes.mappedFile();
        this.indexing = new Indexing(wireType);
    }


    @Override
    public long writePosition() {
        return this.bounds.getWritePosition();
    }

    @Override
    public void writePosition(long position) {
        this.bounds.setWritePosition(position);
    }

    /**
     * @return the file identifier based on the high 24bits of the index, when DAY ROLLING each day
     * will have a unique cycle
     */
    @Override
    public long cycle() {
        return this.roll.cycle();
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
     * @return the first index available on the file system
     */
    @Override
    public long firstSubIndex() {
        return this.indexing.firstSubIndex();
    }

    /**
     * @return the last index available on the file system
     */
    @Override
    public long lastSubIndex() {
        return this.indexing.lastSubIndex();
    }


    @Override
    public boolean appendRollMeta(@NotNull Wire wire, long cycle) throws IOException {
        if (!roll.casNextRollCycle(cycle))
            return false;
        wire.writeDocument(true, d -> d.write(MetaDataField.roll).int32(cycle));
        bounds.setWritePosition(wire.bytes().writePosition());
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

    @Override
    public long refCount() {
        return this.refCount.get();
    }

    // *************************************************************************
    // BOOTSTRAP
    // *************************************************************************

    @Override
    public void install(
            @NotNull MappedBytes mappedBytes,
            long length,
            boolean created,
            long cycle,
            @NotNull ChronicleQueueBuilder builder,
            @NotNull Function<Bytes, Wire> wireSupplier,
            @Nullable Closeable closeable) throws IOException {

        this.builder = (SingleChronicleQueueBuilder) builder;

        if (created) {
            this.bounds.setWritePosition(length);
            this.bounds.setReadPosition(length);
            this.roll.cycle(cycle);
        }
    }


    /**
     * @return creates a new instance of mapped bytes, because, for example the tailer and appender
     * can be at different locations.
     */
    @Override
    public MappedBytes mappedBytes() {
        final MappedBytes mappedBytes = new MappedBytes(mappedFile);//.withSizes(this.chunkSize, this.overlapSize);
        mappedBytes.writePosition(bounds.getWritePosition());
        mappedBytes.readPosition(bounds.getReadPosition());
        return mappedBytes;
    }


    @Override
    public void storeIndexLocation(Wire wire, long position, long subIndex) {
        indexing.storeIndexLocation(wire, position, subIndex);
    }


    // *************************************************************************
    // Utilities
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

    private int toIntU30(long len) {
        return Wires.toIntU30(len, "Document length %,d out of 30-bit int range.");
    }

    // *************************************************************************
    // Utilities :: Read
    // *************************************************************************


    // *************************************************************************
    // Utilities :: Write
    // *************************************************************************


    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(MetaDataField.wireType).object(wireType);
        wire.write(MetaDataField.bounds).marshallable(this.bounds)
                .write(MetaDataField.roll).object(this.roll)
                .write(MetaDataField.chunkSize).int64(mappedFile.chunkSize())
                .write(MetaDataField.overlapSize).int64(mappedFile.overlapSize())
                .write(MetaDataField.indexing).object(this.indexing);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        wireType = wire.read(MetaDataField.wireType).object(WireType.class);
        wire.read(MetaDataField.bounds).marshallable(this.bounds);
        wire.read(MetaDataField.roll).marshallable(this.roll);
        long chunkSize = wire.read(MetaDataField.chunkSize).int64();
        long overlapSize = wire.read(MetaDataField.overlapSize).int64();

        final MappedBytes mappedBytes = (MappedBytes) (wire.bytes());
        this.mappedFile = mappedBytes.mappedFile();
        indexing = new Indexing(wireType);
        wire.read(MetaDataField.indexing).marshallable(indexing);

    }

// *************************************************************************
// Marshallable
// *************************************************************************

    enum MetaDataField implements WireKey {
        bounds,
        indexing,
        roll,
        chunkSize,
        wireType, overlapSize
    }

    enum BoundsField implements WireKey {
        writePosition,
        readPosition,
    }

// *************************************************************************
//
// *************************************************************************

    enum IndexingFields implements WireKey {
        indexCount, indexSpacing, index2Index, fistIndex, lastIndex
    }

    public enum IndexOffset {
        ;

        public static long toAddress0(long index) {

            long siftedIndex = index >> (17L + 6L);
            long mask = (1L << 17L) - 1L;
            long maskedShiftedIndex = mask & siftedIndex;

            // convert to an offset
            return maskedShiftedIndex * 8L;
        }

        public static long toAddress1(long index) {

            long siftedIndex = index >> (6L);
            long mask = (1L << 17L) - 1L;
            long maskedShiftedIndex = mask & siftedIndex;

            // convert to an offset
            return maskedShiftedIndex;// * 8L;
        }

        @NotNull
        public static String toBinaryString(long i) {

            StringBuilder sb = new StringBuilder();

            for (int n = 63; n >= 0; n--)
                sb.append(((i & (1L << n)) != 0 ? "1" : "0"));

            return sb.toString();
        }

        @NotNull
        public static String toScale() {

            StringBuilder units = new StringBuilder();
            StringBuilder tens = new StringBuilder();

            for (int n = 64; n >= 1; n--)
                units.append((0 == (n % 10)) ? "|" : n % 10);

            for (int n = 64; n >= 1; n--)
                tens.append((0 == (n % 10)) ? n / 10 : " ");

            return units.toString() + "\n" + tens.toString();
        }
    }

// *************************************************************************
//
// *************************************************************************

    enum RollFields implements WireKey {
        cycle, length, format, timeZone, nextCycle, epoch, firstCycle, lastCycle, nextCycleMetaPosition
    }

    @FunctionalInterface
    private interface Reader<T> {
        long read(@NotNull Bytes context, int len, @NotNull T reader) throws IOException;
    }

    @FunctionalInterface
    private interface WireWriter<T> {
        long write(@NotNull Wire wire, long position, int size, @NotNull T writer) throws IOException;
    }

    @FunctionalInterface
    private interface BytesWriter<T> {
        long write(@NotNull Bytes wire, long position, int size, @NotNull T writer) throws IOException;
    }

    class Bounds implements Marshallable {
        private LongValue writePosition;
        private LongValue readPosition;

        Bounds() {
            this.writePosition = null;
            this.readPosition = null;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(BoundsField.writePosition).int64forBinding(
                    WireConstants.HEADER_OFFSET, writePosition = wire.newLongReference())
                    .write(BoundsField.readPosition).int64forBinding(
                    WireConstants.HEADER_OFFSET, readPosition = wire.newLongReference());
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            wire.read(BoundsField.writePosition).int64(
                    this.writePosition, this, (o, i) -> o.writePosition = i)
                    .read(BoundsField.readPosition).int64(
                    this.readPosition, this, (o, i) -> o.readPosition = i);
        }

        public long getReadPosition() {
            return this.readPosition.getVolatileValue();
        }

        public void setReadPosition(long position) {
            this.readPosition.setOrderedValue(position);
        }

        public long getWritePosition() {
            return this.writePosition.getVolatileValue();
        }


        public void setWritePosition(long writePosition) {
            for (; ; ) {
                long wp = writePosition();
                if (writePosition > wp) {
                    if (this.writePosition.compareAndSwapValue(wp, writePosition)) {
                        return;
                    }
                } else {
                    break;
                }
            }
        }
    }

// *************************************************************************
//
// *************************************************************************

    class Indexing implements Marshallable {
        //private final MappedBytes indexContext;
        private final Wire templateIndex;
        // hold a reference to it so it doesn't get cleaned up.
        private Bytes<?> mappedBytes;
        private int indexCount = 128 << 10;
        private int indexSpacing = 64;
        private LongValue index2Index;
        private LongValue lastIndex;
        private ThreadLocal<LongArrayValues> longArray;

        private LongValue firstIndex;

        Indexing(@NotNull WireType wireType) {
            this.index2Index = wireType.newLongReference().get();
            this.firstIndex = wireType.newLongReference().get();

            this.lastIndex = wireType.newLongReference().get();
            if (wireType == WireType.TEXT)
                templateIndex = TEXT_TEMPLATE;
            else if (wireType == WireType.BINARY || wireType == WireType.FIELDLESS_BINARY)
                templateIndex = BINARY_TEMPLATE;
            else {
                throw new UnsupportedOperationException("type is not supported");
            }
            this.longArray = withInitial(wireType.newLongArrayReference());
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(IndexingFields.indexCount).int32(indexCount)
                    .write(IndexingFields.indexSpacing).int32(indexSpacing)
                    .write(IndexingFields.index2Index).int64forBinding(0L, index2Index)
                    .write(IndexingFields.fistIndex).int64forBinding(-1L, firstIndex)
                    .write(IndexingFields.lastIndex).int64forBinding(-1L, lastIndex);
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            wire.read(IndexingFields.indexCount).int32(this, (o, i) -> o.indexCount = i)
                    .read(IndexingFields.indexSpacing).int32(this, (o, i) -> o.indexSpacing = i)
                    .read(IndexingFields.index2Index).int64(this.index2Index, this, (o, i) -> o.index2Index = i)
                    .read(IndexingFields.fistIndex).int64(this.firstIndex, this, (o, i) -> o.firstIndex = i)
                    .read(IndexingFields.lastIndex).int64(this.lastIndex, this, (o, i) -> o.lastIndex = i);
        }

        public boolean setLastSubIndexIfGreater(long lastIndex) {
            final long v = this.lastIndex.getVolatileValue();
            return v < lastIndex && this.lastIndex.compareAndSwapValue(v, lastIndex);
        }

        public long lastSubIndex() {
            if (lastIndex == null)
                return 0;
            return this.lastIndex.getVolatileValue();
        }

        public long firstSubIndex() {
            if (index2Index.getVolatileValue() == 0)
                return -1;
            if (firstIndex == null)
                return 0;
            return this.firstIndex.getVolatileValue();
        }


        /**
         * atomically gets or creates the address of the first index the index is create and another
         * except into the queue, however this except is treated as meta data and does not increment
         * the last index, in otherword it is not possible to access this except by calling index(),
         * it effectively invisible to the end-user
         *
         * @param bytes used to write and index if it does not exist
         * @return the position of the index
         */
        long indexToIndex(@Nullable final Bytes bytes) {
            for (; ; ) {
                long index2Index = this.index2Index.getVolatileValue();

                if (index2Index == NOT_READY)
                    continue;

                if (index2Index != NOT_INITIALIZED)
                    return index2Index;

                if (!this.index2Index.compareAndSwapValue(NOT_INITIALIZED, NOT_READY))
                    continue;

                if (bytes == null)
                    return -1;

                final long index = newIndex(bytes);
                this.index2Index.setOrderedValue(index);
                return index;
            }
        }

        /**
         * records the the location of the subIndex, only every 64th address is written to the
         * subIndex file, the first subIndex is stored at {@code index2index}
         *
         * @param wire     the context that we are referring to
         * @param address  the address of the Excerpts which we are going to record
         * @param subIndex the subIndex of the Excerpts which we are going to record
         */
        public void storeIndexLocation(Wire wire,
                                       final long address,
                                       final long subIndex) {

            setLastSubIndexIfGreater(subIndex);

            long writePostion = wire.bytes().writePosition();
            try {
                final Bytes<?> bytes = wire.bytes();

                if (subIndex % 64 != 0)
                    return;

                final LongArrayValues array = this.longArray.get();
                final long indexToIndex0 = indexToIndex(bytes);

                try (DocumentContext _ = wire.readingDocument(indexToIndex0)) {

                    if (!_.isPresent())
                        throw new IllegalStateException("document not found");

                    if (!_.isMetaData())
                        throw new IllegalStateException("subIndex not found");

                    final LongArrayValues primaryIndex = array(wire, array);
                    final long primaryOffset = toAddress0(subIndex);
                    long secondaryAddress = primaryIndex.getValueAt(primaryOffset);

                    if (secondaryAddress == Wires.NOT_INITIALIZED) {
                        secondaryAddress = newIndex(bytes);
                        writePostion = Math.max(writePostion, wire.bytes().writePosition());
                        primaryIndex.setValueAt(primaryOffset, secondaryAddress);
                    }

                    bytes.readLimit(bytes.capacity());
                    try (DocumentContext context = wire.readingDocument(secondaryAddress)) {

                        final LongArrayValues array1 = array(wire, array);
                        if (!context.isPresent())
                            throw new IllegalStateException("document not found");

                        if (!context.isMetaData())
                            throw new IllegalStateException("subIndex not found");
                        array1.setValueAt(toAddress1(subIndex), address);
                    }

                }

            } finally {
                wire.bytes().writePosition(writePostion);
            }
        }

        private LongArrayValues array(WireIn w, LongArrayValues using) {
            final StringBuilder sb = Wires.acquireStringBuilder();
            final ValueIn valueIn = w.readEventName(sb);
            if (!"index".contentEquals(sb))
                throw new IllegalStateException("expecting index");

            valueIn.int64array(using, this, (o1, o2) -> {
            });
            return using;
        }

        /**
         * Creates a new Excerpt containing and index which will be 1L << 17L bytes long, This
         * method is used for creating both the primary and secondary indexes. Chronicle Queue uses
         * a root primary index ( each entry in the primary index points to a unique a secondary
         * index. The secondary index only records the address of every 64th except, the except are
         * linearly scanned from there on.
         *
         * @param bytes
         * @return the address of the Excerpt containing the usable index, just after the header
         */
        long newIndex(Bytes bytes) {

            try {
                final long position = bytes.writePosition();
                bytes.write(templateIndex.bytes());
                return position;
            } catch (Throwable e) {
                throw Jvm.rethrow(e);
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
         * so from every 64th excerpt a linear scan occurs. The indexes are only built when the
         * indexer is run, this could be on a background thread. Each targetIndex is created into
         * chronicle as an excerpt.
         *
         * @param wire  the data structure we are navigating
         * @param index the index we wish to move to
         * @return the position of the {@code targetIndex}  or -1 if the index can not be found
         */
        public long moveToIndex(@NotNull final Wire wire, final long index) {
            final LongArrayValues array = this.longArray.get();
            final Bytes<?> bytes = wire.bytes();
            final long indexToIndex0 = indexToIndex(bytes);


            final long readPostion = bytes.readPosition();
            bytes.readLimit(bytes.capacity()).readPosition(indexToIndex0);
            long startIndex = ((index / 64L)) * 64L;
            //   final long limit = wire.bytes().readLimit();
            try (@NotNull final DocumentContext documentContext0 = wire.readingDocument()) {

                if (!documentContext0.isPresent())
                    throw new IllegalStateException("document is not present");

                if (documentContext0.isData())
                    throw new IllegalStateException("Invalid index, expecting and index at " +
                            "pos=" + indexToIndex0 + ", but found data instead.");

                final LongArrayValues primaryIndex = array(wire, array);
                long primaryOffset = toAddress0(index);

                do {

                    long secondaryAddress = primaryIndex.getValueAt(primaryOffset);
                    if (secondaryAddress == 0) {
                        startIndex -= (1 << 23L);
                        primaryOffset--;
                        continue;
                    }

                    bytes.readLimit(bytes.capacity());
                    bytes.readPosition(secondaryAddress);

                    try (@NotNull final DocumentContext documentContext1 = wire.readingDocument()) {

                        if (!documentContext1.isPresent())
                            throw new IllegalStateException("document is not present");

                        if (documentContext1.isData())
                            continue;

                        final LongArrayValues array1 = array(wire, array);
                        long secondaryOffset = toAddress1(index);

                        do {
                            long fromAddress = array1.getValueAt(secondaryOffset);
                            if (fromAddress == 0) {
                                secondaryOffset--;
                                startIndex -= 64;
                                continue;
                            }

                            if (index == startIndex) {
                                return fromAddress;
                            } else {
                                bytes.readLimit(bounds.getWritePosition());
                                return linearScan(wire, index, startIndex, fromAddress);
                            }

                        } while (secondaryOffset >= 0);

                    }

                    break;

                } while (primaryOffset >= 0);
            }

            bytes.readPosition(readPostion);
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
         * @return > -1, if successful
         * @see net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.Indexing#moveToIndex
         */
        private long linearScan(Wire context, long toIndex, long fromKnownIndex,
                                long knownAddress) {

            final Bytes<?> bytes = context.bytes();

            final long p = bytes.readPosition();
            final long l = bytes.readLimit();
            bytes.readLimit(bytes.capacity());
            bytes.readPosition(knownAddress);

            for (long i = fromKnownIndex; bytes.readRemaining() > 0; ) {

                // ait until ready - todo add timeout
                for (; ; ) {
                    if (Wires.isReady(bytes.readVolatileInt(bytes.readPosition()))) {

                        break;
                    } else
                        Thread.yield();
                }

                try (@NotNull final DocumentContext documentContext = context.readingDocument()) {

                    if (!documentContext.isPresent())
                        throw new IllegalStateException("document is not present");

                    if (!documentContext.isData())
                        continue;

                    if (toIndex == i) {
                        context.bytes().readSkip(-4);
                        final long readPosition = context.bytes().readPosition();
                        return readPosition;
                    }
                    i++;
                }
            }
            bytes.readLimit(l).readPosition(p);
            return -1;
        }
    }

// *************************************************************************
//
// *************************************************************************

    class Roll implements Marshallable {
        private long epoch;
        private int length;
        private String format;
        private ZoneId zoneId;
        private LongValue cycle;
        private LongValue nextCycle;
        private LongValue nextCycleMetaPosition;


        Roll(RollCycle rollCycle, long rollEpoc) {
            this.length = rollCycle != null ? rollCycle.length() : -1;
            this.format = rollCycle != null ? rollCycle.format() : null;
            this.zoneId = rollCycle != null ? rollCycle.zone() : null;
            this.epoch = rollEpoc;
            this.cycle = null;
            this.nextCycle = null;
            this.nextCycleMetaPosition = null;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(RollFields.cycle).int64forBinding(-1, cycle = wire.newLongReference())
                    .write(RollFields.length).int32(length)
                    .write(RollFields.format).text(format)
                    .write(RollFields.timeZone).text(zoneId.getId())
                    .write(RollFields.nextCycle).int64forBinding(-1, nextCycle = wire.newLongReference())
                    .write(RollFields.epoch).int64(epoch)
                    .write(RollFields.nextCycleMetaPosition).int64forBinding(-1, nextCycleMetaPosition = wire.newLongReference());
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            wire.read(RollFields.cycle).int64(this.cycle, this, (o, i) -> o.cycle = i)
                    .read(RollFields.length).int32(this, (o, i) -> o.length = i)
                    .read(RollFields.format).text(this, (o, i) -> o.format = i)
                    .read(RollFields.timeZone).text(this, (o, i) -> o.zoneId = ZoneId.of(i))
                    .read(RollFields.nextCycle).int64(this.nextCycle, this, (o, i) -> o.nextCycle = i)
                    .read(RollFields.epoch).int64(this, (o, i) -> o.epoch = i)
                    .read(RollFields.nextCycleMetaPosition).int64(this.nextCycleMetaPosition, this, (o, i) -> o.nextCycleMetaPosition = i);
        }

        /**
         * @return an epoch offset as the number of number of milliseconds since January 1, 1970,
         * 00:00:00 GMT
         */
        public long epoch() {
            return this.epoch;
        }


        public long cycle() {
            return this.cycle.getVolatileValue();
        }

        public Roll cycle(long rollCycle) {
            this.cycle.setOrderedValue(rollCycle);
            return this;
        }

        public Roll nextCycleMetaPosition(long position) {
            this.nextCycleMetaPosition.setOrderedValue(position);
            return this;
        }

        public boolean casNextRollCycle(long rollCycle) {
            return this.nextCycle.compareAndSwapValue(-1, rollCycle);
        }
    }
}
