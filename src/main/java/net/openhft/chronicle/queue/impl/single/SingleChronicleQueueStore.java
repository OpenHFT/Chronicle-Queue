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
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;

import static java.lang.ThreadLocal.withInitial;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.IndexOffset.toAddress0;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.IndexOffset.toAddress1;
import static net.openhft.chronicle.wire.Wires.NOT_INITIALIZED;
import static net.openhft.chronicle.wire.Wires.NOT_READY;

public class SingleChronicleQueueStore implements WireStore {

    private static final long NUMBER_OF_ENTRIES_IN_EACH_INDEX = 1 << 17;

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Bounds.class, "Bounds");
        ClassAliasPool.CLASS_ALIASES.addAlias(Indexing.class, "Indexing");
        ClassAliasPool.CLASS_ALIASES.addAlias(Roll.class, "Roll");
    }

    @NotNull
    private WireType wireType;
    @NotNull
    private final Roll roll;
    @NotNull
    private final Bounds bounds = new Bounds();
    private MappedFile mappedFile;
    @Nullable
    private Closeable resourceCleaner;
    private final ReferenceCounter refCount = ReferenceCounter.onReleased(this::performRelease);
    @Nullable
    private Indexing indexing;
    public static final WriteMarshallable INDEX_TEMPLATE = w -> w.writeEventName(() -> "index")
            .int64array(NUMBER_OF_ENTRIES_IN_EACH_INDEX);

    /**
     * Default constructor needed for self boot-strapping
     */
    SingleChronicleQueueStore() {
        this.roll = new Roll(null, 0);
        this.wireType = WireType.BINARY;
    }

    /**
     * @param rollCycle   the current rollCycle
     * @param wireType    the wire type that is being used
     * @param mappedBytes used to mapped the data store file
     * @param rollEpoc    sets an epoch offset as the number of number of milliseconds since January
     *                    1, 1970,  00:00:00 GMT
     */
    SingleChronicleQueueStore(@Nullable RollCycle rollCycle,
                              @NotNull final WireType wireType,
                              @NotNull MappedBytes mappedBytes,
                              long rollEpoc) {
        this.roll = new Roll(rollCycle, rollEpoc);
        this.resourceCleaner = null;
        this.wireType = wireType;
        this.mappedFile = mappedBytes.mappedFile();
        this.indexing = new Indexing(wireType);
    }

    @Override
    public long writePosition() {
        return this.bounds.writePosition();
    }

    @Override
    public WireStore writePosition(long position) {
        this.bounds.writePosition(position);
        return this;
    }

    /**
     * @return the file identifier based on the high 24bits of the index, when DAY ROLLING each day
     * will have a unique cycle
     */
    @Override
    public long cycle() {
        final Roll roll = this.roll;
        return roll.cycle();
    }

    /**
     * @return an epoch offset as the number of number of milliseconds since January 1, 1970,
     * 00:00:00 GMT
     */
    @Override
    public long epoch() {
        final Roll roll = this.roll;
        return roll.epoch();
    }

    /**
     * @return the first index available on the file system
     */
    @Override
    public long firstSequenceNumber() {
        if (this.indexing == null)
            return 0;
        return this.indexing.firstSequenceNumber();

    }

    /**
     * @return the last index available on the file system
     */
    @Override
    public long sequenceNumber() {
        if (this.indexing == null)
            return 0;
        return this.indexing.lastSequenceNumber();
    }

    @Override
    public boolean appendRollMeta(@NotNull Wire wire, long cycle) {
        if (!roll.casNextRollCycle(cycle))
            return false;
        wire.writeDocument(true, d -> d.write(MetaDataField.roll).int32(cycle));
        bounds.writePosition(wire.bytes().writePosition());
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
        if (indexing == null)
            return -1;
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
    public void install(long length, boolean created, long cycle,
                        @NotNull ChronicleQueueBuilder builder) {
        if (created) {
            this.bounds.writePosition(length);
            this.bounds.readPosition(length);
            this.roll.cycle(cycle);
        }
    }


    /**
     * @return creates a new instance of mapped bytes, because, for example the tailer and appender
     * can be at different locations.
     */
    @NotNull
    @Override
    public MappedBytes mappedBytes() {
        return new MappedBytes(mappedFile);//.withSizes(this.chunkSize, this.overlapSize);
    }

    @Override
    public void storeIndexLocation(@NotNull Wire wire, long position, long sequenceNumber) {
        indexing.storeIndexLocation(wire, position, sequenceNumber);
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
        wire.read(MetaDataField.chunkSize).int64();
        wire.read(MetaDataField.overlapSize).int64();

        @NotNull final MappedBytes mappedBytes = (MappedBytes) (wire.bytes());
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

            // convert to an offset
            return mask & siftedIndex;// * 8L;
        }

        @NotNull
        public static String toBinaryString(long i) {

            @NotNull StringBuilder sb = new StringBuilder();

            for (int n = 63; n >= 0; n--)
                sb.append(((i & (1L << n)) != 0 ? "1" : "0"));

            return sb.toString();
        }

        @NotNull
        public static String toScale() {

            @NotNull StringBuilder units = new StringBuilder();
            @NotNull StringBuilder tens = new StringBuilder();

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


    class Bounds implements Marshallable {
        @Nullable
        private LongValue writePosition;
        @Nullable
        private LongValue readPosition;

        Bounds() {
            this.writePosition = null;
            this.readPosition = null;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(BoundsField.writePosition).int64forBinding(0, writePosition = wire
                    .newLongReference())
                    .write(BoundsField.readPosition).int64forBinding(0, readPosition = wire
                    .newLongReference());
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            wire.read(BoundsField.writePosition).int64(
                    this.writePosition, this, (o, i) -> o.writePosition = i)
                    .read(BoundsField.readPosition).int64(
                    this.readPosition, this, (o, i) -> o.readPosition = i);
        }

        public long readPosition() {
            return this.readPosition.getVolatileValue();
        }

        public void readPosition(long position) {
            this.readPosition.setOrderedValue(position);
        }

        public long writePosition() {
            return this.writePosition.getVolatileValue();
        }

        public void writePosition(long writePosition) {
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

        @Override
        public String toString() {
            return "Bounds{" +
                    "writePosition=" + writePosition +
                    ", readPosition=" + readPosition +
                    '}';
        }
    }

// *************************************************************************
//
// *************************************************************************

    class Indexing implements Marshallable {

        private int indexCount = 128 << 10;
        private int indexSpacing = 64;
        private LongValue index2Index;
        private LongValue lastSequenceNumber;
        private ThreadLocal<LongArrayValues> longArray;

        private LongValue firstIndex;

        Indexing(@NotNull WireType wireType) {
            this.index2Index = wireType.newLongReference().get();
            this.firstIndex = wireType.newLongReference().get();
            this.lastSequenceNumber = wireType.newLongReference().get();
            this.longArray = withInitial(wireType.newLongArrayReference());
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(IndexingFields.indexCount).int32(indexCount)
                    .write(IndexingFields.indexSpacing).int32(indexSpacing)
                    .write(IndexingFields.index2Index).int64forBinding(0L, index2Index)
                    .write(IndexingFields.fistIndex).int64forBinding(-1L, firstIndex)
                    .write(IndexingFields.lastIndex).int64forBinding(-1L, lastSequenceNumber);
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            wire.read(IndexingFields.indexCount).int32(this, (o, i) -> o.indexCount = i)
                    .read(IndexingFields.indexSpacing).int32(this, (o, i) -> o.indexSpacing = i)
                    .read(IndexingFields.index2Index).int64(this.index2Index, this, (o, i) -> o.index2Index = i)
                    .read(IndexingFields.fistIndex).int64(this.firstIndex, this, (o, i) -> o.firstIndex = i)
                    .read(IndexingFields.lastIndex).int64(this.lastSequenceNumber, this, (o, i) -> o.lastSequenceNumber = i);
        }

        public boolean lastSequenceNumber(long lastIndex) {
            final long v = this.lastSequenceNumber.getVolatileValue();
            return v < lastIndex && this.lastSequenceNumber.compareAndSwapValue(v, lastIndex);
        }

        public long lastSequenceNumber() {
            if (lastSequenceNumber == null)
                return 0;
            return this.lastSequenceNumber.getVolatileValue();
        }

        public long firstSequenceNumber() {
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
         * @param wire the current wire
         * @return the position of the index
         */
        long indexToIndex(@NotNull final Wire wire) {
            for (; ; ) {
                long index2Index = this.index2Index.getVolatileValue();

                if (index2Index == NOT_READY)
                    continue;

                if (index2Index != NOT_INITIALIZED)
                    return index2Index;

                if (!this.index2Index.compareAndSwapValue(NOT_INITIALIZED, NOT_READY))
                    continue;

                final long index = newIndex(wire);
                this.index2Index.setOrderedValue(index);
                return index;
            }
        }

        /**
         * records the the location of the sequenceNumber, only every 64th address is written to the
         * sequenceNumber file, the first sequenceNumber is stored at {@code index2index}
         *
         * @param wire           the context that we are referring to
         * @param address        the address of the Excerpts which we are going to record
         * @param sequenceNumber the sequenceNumber of the Excerpts which we are going to record
         */
        public void storeIndexLocation(@NotNull Wire wire,
                                       final long address,
                                       final long sequenceNumber) {

            lastSequenceNumber(sequenceNumber);

            long writePosition = wire.bytes().writePosition();
            try {
                @NotNull final Bytes<?> bytes = wire.bytes();

                if (sequenceNumber % 64 != 0)
                    return;

                final LongArrayValues array = this.longArray.get();
                final long indexToIndex0 = indexToIndex(wire);

                try (DocumentContext context = wire.readingDocument(indexToIndex0)) {

                    if (!context.isPresent())
                        throw new IllegalStateException("document not found");

                    if (!context.isMetaData())
                        throw new IllegalStateException("sequenceNumber not found");

                    @NotNull final LongArrayValues primaryIndex = array(wire, array);
                    final long primaryOffset = toAddress0(sequenceNumber);
                    long secondaryAddress = primaryIndex.getValueAt(primaryOffset);

                    if (secondaryAddress == Wires.NOT_INITIALIZED) {
                        secondaryAddress = newIndex(wire);
                        writePosition = Math.max(writePosition, wire.bytes().writePosition());
                        primaryIndex.setValueAt(primaryOffset, secondaryAddress);
                    }

                    bytes.readLimit(bytes.capacity());
                    try (DocumentContext context0 = wire.readingDocument(secondaryAddress)) {

                        @NotNull final LongArrayValues array1 = array(wire, array);
                        if (!context0.isPresent())
                            throw new IllegalStateException("document not found");
                        if (!context0.isMetaData())
                            throw new IllegalStateException("sequenceNumber not found");
                        array1.setValueAt(toAddress1(sequenceNumber), address);
                    }

                }

            } finally {
                wire.bytes().writePosition(writePosition);
            }
        }

        @NotNull
        private LongArrayValues array(@NotNull WireIn w, @NotNull LongArrayValues using) {
            final StringBuilder sb = Wires.acquireStringBuilder();
            @NotNull final ValueIn valueIn = w.readEventName(sb);
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
         * linearly scanned from there on.  )
         *
         * @param wire the current wire
         * @return the address of the Excerpt containing the usable index, just after the header
         */
        long newIndex(@NotNull Wire wire) {
            long position;
            do {
                position = WireInternal.writeDataOrAdvanceIfNotEmpty(wire, true, INDEX_TEMPLATE);
            } while (position <= 0);

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
         * @return the position of the {@code targetIndex}  or -1 if the index can not be found
         */
        public long moveToIndex(@NotNull final Wire wire, final long index) {
            final LongArrayValues array = this.longArray.get();
            @NotNull final Bytes<?> bytes = wire.bytes();
            final long indexToIndex0 = indexToIndex(wire);
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

                @NotNull final LongArrayValues primaryIndex = array(wire, array);
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

                        @NotNull final LongArrayValues array1 = array(wire, array);
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
                                bytes.readLimit(bounds.writePosition());
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
        private long linearScan(@NotNull Wire context, long toIndex, long fromKnownIndex,
                                long knownAddress) {

            @NotNull
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
                        return context.bytes().readPosition();
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
        @Nullable
        private String format;
        @Nullable
        private ZoneId zoneId;
        @Nullable
        private LongValue cycle;
        @Nullable
        private LongValue nextCycle;
        @Nullable
        private LongValue nextCycleMetaPosition;


        Roll(@Nullable RollCycle rollCycle, long rollEpoch) {
            this.length = rollCycle != null ? rollCycle.length() : -1;
            this.format = rollCycle != null ? rollCycle.format() : null;
            this.zoneId = rollCycle != null ? rollCycle.zone() : null;
            this.epoch = rollEpoch;
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

        @NotNull
        public Roll cycle(long rollCycle) {
            this.cycle.setOrderedValue(rollCycle);
            return this;
        }

        @NotNull
        public Roll nextCycleMetaPosition(long position) {
            this.nextCycleMetaPosition.setOrderedValue(position);
            return this;
        }

        public boolean casNextRollCycle(long rollCycle) {
            return this.nextCycle.compareAndSwapValue(-1, rollCycle);
        }
    }
}
