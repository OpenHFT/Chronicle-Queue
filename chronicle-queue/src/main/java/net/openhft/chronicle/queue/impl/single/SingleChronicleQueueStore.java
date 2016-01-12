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

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.annotation.ForceInline;
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
import static net.openhft.chronicle.queue.impl.WireConstants.SPB_DATA_HEADER_SIZE;
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

    private final WireType wireType;
    private final Roll roll;
    Bounds bounds = new Bounds();
    private MappedBytes mappedBytes;
    private Closeable resourceCleaner;
    private final ReferenceCounter refCount = ReferenceCounter.onReleased(this::performRelease);
    private long appendTimeout = 1_000;
    private SingleChronicleQueueBuilder builder;
    private Indexing indexing;

    /**
     * Default constructor needed for self boot-strapping
     */
    SingleChronicleQueueStore() {
        this.wireType = WireType.BINARY;
        this.roll = new Roll(null, 0);
    }

    /**
     * @param rollCycle
     * @param wireType
     * @param mappedBytes
     * @param rollEpoc    sets an epoc offset as the number of number of milliseconds since January
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
        this.mappedBytes = mappedBytes;
        this.indexing = new Indexing(wireType, mappedBytes);
    }

    @Override
    public long readPosition() {
        return this.bounds.getReadPosition();
    }

    @Override
    public long writePosition() {
        return this.bounds.getWritePosition();
    }

    @Override
    public long cycle() {
        return this.roll.cycle();
    }

    /**
     * @return an epoc offset as the number of number of milliseconds since January 1, 1970,
     * 00:00:00 GMT
     */
    @Override
    public long epoc() {
        return this.roll.epoc();
    }

    @Override
    public long lastIndex() {
        return this.indexing.getLastIndex();
    }

    @Override
    public long append(@NotNull Wire wire, @NotNull final WriteMarshallable marshallable) throws IOException {
        return write(wire, Wires.UNKNOWN_LENGTH, this::writeWireMarshallable, marshallable);
    }

    @Override
    public long append(@NotNull Wire wire, @NotNull final WriteBytesMarshallable marshallable) throws IOException {
        return write(wire, Wires.UNKNOWN_LENGTH, this::writeBytesMarshallable, marshallable);
    }

    @Override
    public long append(@NotNull Wire wire, @NotNull final Bytes bytes) throws IOException {
        return write(wire.bytes(), toIntU30(bytes.length()), this::writeBytes, bytes);
    }

    @Override
    public long read(@NotNull Wire wire, @NotNull ReadMarshallable reader) throws IOException {
        return read(wire, this::readWireMarshallable, reader);
    }

    private <T> long readWireMarshallable(Bytes mappedBytes, long l, T t) {
        return 0;
    }

    @Override
    public long read(@NotNull Wire wire, @NotNull ReadBytesMarshallable reader) throws IOException {
        return read(wire, this::readBytesMarshallable, reader);
    }

    @Override
    public boolean appendRollMeta(@NotNull Wire wire, long cycle) throws IOException {
        if (roll.casNextRollCycle(cycle)) {
            final WriteMarshallable marshallable = x -> x.write(MetaDataField.roll).int32(cycle);

            final WireWriter<WriteMarshallable> wireWriter = (context, position, size, writer1) -> {
                // todo improve this line
                Wires.writeMeta(wire, writer1);
                roll.nextCycleMetaPosition(position);
                return WireConstants.NO_INDEX;
            };

            write(wire, Wires.UNKNOWN_LENGTH, wireWriter, marshallable);
            return true;
        }

        return false;
    }

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
            ChronicleQueueBuilder builder,
            @NotNull Function<Bytes, Wire> wireSupplier,
            @Nullable Closeable closeable) throws IOException {

        this.builder = (SingleChronicleQueueBuilder) builder;
        this.appendTimeout = ((SingleChronicleQueueBuilder) builder).appendTimeout();
        if (created) {
            this.bounds.setWritePosition(length);
            this.bounds.setReadPosition(length);
            this.roll.cycle(cycle);
        }
    }

    @Override
    public MappedBytes mappedBytes() {
        return new MappedBytes(mappedBytes.mappedFile());
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

    private long readWireMarshallable(
            @NotNull MappedBytes context,
            int len,
            @NotNull ReadMarshallable marshaller) {

        context.readSkip(SPB_DATA_HEADER_SIZE);
        return readWire(wireType.apply(context), len, marshaller);
    }

    private long readBytesMarshallable(
            @NotNull Bytes context,
            int len,
            @NotNull ReadBytesMarshallable marshaller) {

        context.readSkip(SPB_DATA_HEADER_SIZE);
        final long readp = context.readPosition();
        final long readl = context.readLimit();

        marshaller.readMarshallable(context);
        context.readPosition(readp + len);
        context.readLimit(readl);

        return readp + len;
    }

    private <T> long read(
            @NotNull Wire wire,
            @NotNull Reader<T> reader,
            @NotNull T marshaller) throws IOException {


        final Bytes<?> context = wire.bytes();
        for (; wire.bytes().readRemaining() > 0; ) {


            final int spbHeader = context.readVolatileInt(context.readPosition());

            if (Wires.isNotInitialized(spbHeader) || !Wires.isReady(spbHeader)) {
                Thread.yield();
                continue;
            }

            try (@NotNull final DocumentContext documentContext = wire.readingDocument()) {
                if (!documentContext.isPresent())
                    throw new IllegalStateException("document not present");

                if (documentContext.isData()) {

                    ((ReadMarshallable) marshaller).readMarshallable(wire);
                    return wire.bytes().readPosition();
                }

                // In case of meta data, if we are found the "roll" meta, we returns
                // the next cycle (negative)
                final StringBuilder sb = Wires.acquireStringBuilder();

                // todo improve this line
                final ValueIn vi = wireType.apply(context).read(sb);

                if ("index".contentEquals(sb)) {
                    return read(wire, reader, marshaller);
                } else if ("roll".contentEquals(sb)) {
                    return -vi.int32();
                }

            }

        }

        return WireConstants.NO_DATA;
    }

    //TODO : maybe move to wire
    @ForceInline
    private long readWire(@NotNull WireIn wireIn, long size,
                          @NotNull ReadMarshallable dataConsumer) {
        final Bytes<?> bytes = wireIn.bytes();
        final long limit0 = bytes.readLimit();
        final long limit = bytes.readPosition() + size;
        try {
            bytes.readLimit(limit);
            dataConsumer.readMarshallable(wireIn);
        } finally {
            bytes.readLimit(limit0);
            bytes.readPosition(limit);
        }

        return bytes.readPosition();
    }

    private long writeWireMarshallable(
            @NotNull Wire wire,
            long position,
            int size,
            @NotNull final WriteMarshallable marshallable) throws IOException {

        final long positionDataWritten = Wires.writeData(wire, marshallable);
        final Bytes<?> context = wire.bytes();
        // todo improve this line
        bounds.setWritePositionIfGreater(context.writePosition());

        final long index = indexing.incrementLastIndex();
        indexing.storeIndexLocation(context, positionDataWritten, index);
        return index;
    }

    // *************************************************************************
    // Utilities :: Write
    // *************************************************************************


    private long writeBytesMarshallable(
            @NotNull Wire wire,
            long position,
            int size,
            @NotNull final WriteBytesMarshallable marshallable) throws IOException {

        final Bytes<?> context = wire.bytes();
        context.writeSkip(SPB_DATA_HEADER_SIZE);

        marshallable.writeMarshallable(context);
        context.compareAndSwapInt(
                position,
                Wires.NOT_READY,
                toIntU30(context.writePosition() - position - SPB_DATA_HEADER_SIZE)
        );

        bounds.setWritePositionIfGreater(position);
        final long index = indexing.incrementLastIndex();
        indexing.storeIndexLocation(context, position, index);
        return index;
    }

    private long writeBytes(
            @NotNull Bytes context,
            long position,
            int size,
            @NotNull final Bytes bytes) throws IOException {

        context.writeSkip(4);
        context.write(bytes);
        context.compareAndSwapInt(position, size | Wires.NOT_READY, size);

        final long index = indexing.incrementLastIndex();
        indexing.storeIndexLocation(context, position, index);
        return index;
    }


    <T> long write(
            @NotNull Wire wire,
            int size,
            @NotNull WireWriter<T> wireWriter,
            @NotNull T marshaller) throws IOException {

        final long end = System.currentTimeMillis() + appendTimeout;
        long position = writePosition();

        final Bytes<?> context = wire.bytes();
        for (; ; ) {

            context.writeLimit(context.capacity());
            context.writePosition(position);

            if (context.compareAndSwapInt(position, Wires.NOT_INITIALIZED, Wires.NOT_READY | size)) {
                return wireWriter.write(wire, position, size, marshaller);
            } else {
                int spbHeader = context.readInt(position);
                if (Wires.isKnownLength(spbHeader)) {
                    position += Wires.lengthOf(spbHeader) + SPB_DATA_HEADER_SIZE;
                } else {
                    // TODO: wait strategy
                    if (System.currentTimeMillis() > end) {
                        throw new AssertionError("Timeout waiting to append");
                    }

                    Jvm.pause(1);
                }
            }
        }
    }


    <T> long write(
            @NotNull Bytes bytes,
            int size,
            @NotNull BytesWriter<T> writer,
            @NotNull T marshaller) throws IOException {

        final long end = System.currentTimeMillis() + appendTimeout;
        long position = writePosition();

        for (; ; ) {

            bytes.writeLimit(bytes.capacity());
            bytes.writePosition(position);

            if (bytes.compareAndSwapInt(position, Wires.NOT_INITIALIZED, Wires.NOT_READY | size)) {
                return writer.write(bytes, position, size, marshaller);
            } else {
                int spbHeader = bytes.readInt(position);
                if (Wires.isKnownLength(spbHeader)) {
                    position += Wires.lengthOf(spbHeader) + SPB_DATA_HEADER_SIZE;
                } else {
                    // TODO: wait strategy
                    if (System.currentTimeMillis() > end) {
                        throw new AssertionError("Timeout waiting to append");
                    }

                    Jvm.pause(1);
                }
            }
        }
    }


    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        MappedFile mappedFile = mappedBytes.mappedFile();
        wire.write(MetaDataField.bounds).marshallable(this.bounds)
                .write(MetaDataField.roll).object(this.roll)
                .write(MetaDataField.chunkSize).int64(mappedFile.chunkSize())
                .write(MetaDataField.overlapSize).int64(mappedFile.overlapSize())
                .write(MetaDataField.indexing).object(this.indexing);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {

        //  System.out.println(Wires.fromSizePrefixedBlobs(wire.bytes()));

        wire.read(MetaDataField.bounds).marshallable(this.bounds);
        wire.read(MetaDataField.roll).marshallable(this.roll);
        long chunkSize = wire.read(MetaDataField.chunkSize).int64();
        long overlapSize = wire.read(MetaDataField.overlapSize).int64();

        this.mappedBytes = (MappedBytes) wire.bytes();

        final MappedBytes mappedBytes = new MappedBytes(this.mappedBytes.mappedFile());
        indexing = new Indexing(wireType, mappedBytes.withSizes(chunkSize, overlapSize));
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
        overlapSize
    }

    enum BoundsField implements WireKey {
        writePosition,
        readPosition,
    }

// *************************************************************************
//
// *************************************************************************

    enum IndexingFields implements WireKey {
        indexCount, indexSpacing, index2Index, lastIndex
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
        cycle, length, format, timeZone, nextCycle, epoc, nextCycleMetaPosition
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

        public void setWritePosition(long position) {
            this.writePosition.setOrderedValue(position);
        }

        public void setWritePositionIfGreater(long writePosition) {
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
        private final WireType wireType;
        private final MappedBytes indexContext;
        private final Wire templateIndex;
        private int indexCount = 128 << 10;
        private int indexSpacing = 64;
        private LongValue index2Index;
        private LongValue lastIndex;
        private ThreadLocal<LongArrayValues> longArray;
        private long startIndex;

        Indexing(@NotNull WireType wireType, final MappedBytes mappedBytes) {
            this.index2Index = wireType.newLongReference().get();
            this.lastIndex = wireType.newLongReference().get();

            final Bytes b = Bytes.elasticByteBuffer();

            templateIndex = wireType.apply(b);
            templateIndex.writeDocument(true, w -> w.write(() -> "index")
                    .int64array(NUMBER_OF_ENTRIES_IN_EACH_INDEX));

            this.wireType = wireType;
            this.longArray = withInitial(wireType.newLongArrayReference());
            this.indexContext = mappedBytes;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            //      System.out.println("writeMarshallable");
            wire.write(IndexingFields.indexCount).int32(indexCount)
                    .write(IndexingFields.indexSpacing).int32(indexSpacing)
                    .write(IndexingFields.index2Index).int64forBinding(0L, index2Index)
                    .write(IndexingFields.lastIndex).int64forBinding(-1L, lastIndex);
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {

            //  System.out.println("readMarshallable");
            wire.read(IndexingFields.indexCount).int32(this, (o, i) -> o.indexCount = i)
                    .read(IndexingFields.indexSpacing).int32(this, (o, i) -> o.indexSpacing = i)
                    .read(IndexingFields.index2Index).int64(this.index2Index, this, (o, i) -> o.index2Index = i)
                    .read(IndexingFields.lastIndex).int64(this.lastIndex, this, (o, i) -> o.lastIndex = i);
        }

        public long incrementLastIndex() {
            if (lastIndex == null)
                return 0;
            return this.lastIndex.addAtomicValue(1);
        }

        public long getLastIndex() {
            if (lastIndex == null)
                return 0;
            return this.lastIndex.getVolatileValue();
        }

        /**
         * atomically gets or creates the address of the first index the index is create and another
         * except into the queue, however this except is treated as meta data and does not increment
         * the last index, in otherword it is not possible to access this except by calling index(),
         * it effectively invisible to the end-user
         *
         * @param writeContext used to write and index if it does not exist
         * @return the position of the index
         */
        long indexToIndex(@Nullable final Bytes writeContext) {
            for (; ; ) {
                long index2Index = this.index2Index.getVolatileValue();

                if (index2Index == NOT_READY)
                    continue;

                if (index2Index != NOT_INITIALIZED)
                    return index2Index;

                if (!this.index2Index.compareAndSwapValue(NOT_INITIALIZED, NOT_READY))
                    continue;

                if (writeContext == null)
                    return -1;

                final long index = newIndex(writeContext);
                this.index2Index.setOrderedValue(index);
                return index;
            }
        }

        /**
         * records the the location of the index, only every 64th address is written to the index
         * file, the first index is stored at {@code index2index}
         *
         * @param context the context that we are referring to
         * @param address the address of the Excerpts which we are going to record
         * @param index   the index of the Excerpts which we are going to record
         */
        public void storeIndexLocation(Bytes context,
                                       final long address,
                                       final long index) throws IOException {

            if (index % 64 != 0)
                return;

            final LongArrayValues array = this.longArray.get();
            final long indexToIndex0 = indexToIndex(context);

            final MappedBytes indexBytes = indexContext;
            indexBytes.readLimit(indexBytes.capacity());
            final Bytes bytes0 = indexBytes.readPosition(indexToIndex0);
            final Wire w = wireType.apply(bytes0);

            final long l = w.bytes().readPosition();
            w.readDocument(d -> {

                final LongArrayValues primaryIndex = array(d, array);
                final long primaryOffset = toAddress0(index);
                long secondaryAddress = primaryIndex.getValueAt(primaryOffset);

                if (secondaryAddress == Wires.NOT_INITIALIZED) {
                    secondaryAddress = newIndex(context);
                    primaryIndex.setValueAt(primaryOffset, secondaryAddress);
                }

                indexBytes.readLimit(indexBytes.capacity());
                final Bytes bytes = indexBytes.readPosition(secondaryAddress);
                final Wire wire1 = wireType.apply(bytes);
                wire1.readDocument(document -> {
                    final LongArrayValues array1 = array(document, array);
                    array1.setValueAt(toAddress1(index), address);
                }, null);

            }, null);

        }

        private LongArrayValues array(WireIn w, LongArrayValues using) {
            final ValueIn read = w.read(() -> "index");

            read.int64array(using, this, (o1, o2) -> {
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
         * @param writeContext
         * @return the address of the Excerpt containing the usable index, just after the header
         */
        long newIndex(Bytes writeContext) {

            try {
                final long start = writeContext.writePosition() + SPB_DATA_HEADER_SIZE;
                final Bytes<?> bytes = templateIndex.bytes();
                write(writeContext, toIntU30((long) bytes.length()) | Wires.META_DATA, this::writeIndexBytes, bytes);
                return start;
            } catch (Throwable e) {
                throw Jvm.rethrow(e);
            }
        }

        private long writeIndexBytes(
                @NotNull Bytes target,
                long position,
                int size,
                @NotNull final Bytes sourceBytes) throws IOException {

            target.writeSkip(4);
            sourceBytes.writeLimit(size);
            target.write(sourceBytes);
            target.compareAndSwapInt(position, size | Wires.NOT_READY, size);

            // we don't want to index the meta data
            if (Wires.isData(sourceBytes.readLong(sourceBytes.readPosition()))) {
                final long index = indexing.incrementLastIndex();
                indexing.storeIndexLocation(target, position, index);
                return index;
            }

            return -1;


        }


        /**
         * The indexes are stored in many excerpts, so the index2index tells chronicle where ( in
         * other words the address of where ) the root first level targetIndex is stored. The
         * indexing works like a tree, but only 2 levels deep, the root of the tree is at
         * index2index ( this first level targetIndex is 1MB in size and there is only one of them,
         * it only holds the addresses of the second level indexes, there will be many second level
         * indexes ( created on demand ), each is about 1MB in size  (this second level targetIndex
         * only stores the position of every 64th excerpt), so from every 64th excerpt a linear scan
         * occurs. The indexes are only built when the indexer is run, this could be on a background
         * thread. Each targetIndex is created into chronicle as an excerpt.
         */
        public long moveToIndex(Wire wire, final long targetIndex) {
            final LongArrayValues array = this.longArray.get();
            final long indexToIndex0 = indexToIndex(wire.bytes());

            final Bytes<?> bytes = wire.bytes();
            bytes.readLimit(indexContext.capacity()).readPosition(indexToIndex0);

            this.startIndex = ((targetIndex / 64L)) * 64L;
            long result = bytes.readPosition();

            try (@NotNull final DocumentContext documentContext0 = wire.readingDocument()) {

                if (!documentContext0.isPresent())
                    throw new IllegalStateException("document is not present");

                if (documentContext0.isData())
                    throw new IllegalStateException("Invalid index, expecting and index at " +
                            "pos=" + indexToIndex0 + ", but found data instead.");

                final LongArrayValues primaryIndex = array(wire, array);
                long primaryOffset = toAddress0(targetIndex);

                do {

                    long secondaryAddress = primaryIndex.getValueAt(primaryOffset);
                    if (secondaryAddress == 0) {
                        startIndex -= (1 << 23L);
                        primaryOffset--;
                        System.out.println("SECONDARY INDEX NOT FOUND ! - its going to be a long " +
                                "linuar scan !");
                        continue;
                    }

                    //indexContext.readLimit(indexContext.capacity());
                    final Wire wire1 = wireType.apply(indexContext.readPosition(secondaryAddress));

                    final long limit = wire1.bytes().readLimit();

                    try (@NotNull final DocumentContext documentContext1 = wire1.readingDocument()) {

                        if (!documentContext1.isPresent())
                            throw new IllegalStateException("document is not present");

                        if (documentContext1.isData())
                            continue;

                        final LongArrayValues array1 = array(wire1, array);
                        long secondaryOffset = toAddress1(targetIndex);

                        do {
                            long fromAddress = array1.getValueAt(secondaryOffset);
                            if (fromAddress == 0) {
                                secondaryOffset--;
                                startIndex -= 64;
                                System.out.println("SECONDARY INDEX NOT FOUND !");
                                continue;
                            }

                            if (targetIndex == startIndex) {
                                return fromAddress;
                            } else {
                                wire1.bytes().readLimit(limit);
                                return linearScan(wire1, targetIndex, startIndex, fromAddress);
                            }

                        } while (secondaryOffset >= 0);

                    }

                    break;

                } while (primaryOffset >= 0);
            }


            return result;

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
        private long linearScan(Wire context, long toIndex, long fromKnownIndex, long knownAddress) {

            final Bytes<?> bytes = context.bytes();

            final long p = bytes.readPosition();
            final long l = bytes.readLimit();

            bytes.readPosition(knownAddress);

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
                        throw new IllegalStateException("document is not present");

                    if (!documentContext.isData())
                        continue;

                    if (toIndex == i)
                        return context.bytes().readPosition() - 4;
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
        private long epoc;
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
            this.epoc = rollEpoc;
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
                    .write(RollFields.epoc).int64(epoc)
                    .write(RollFields.nextCycleMetaPosition).int64forBinding(-1, nextCycleMetaPosition = wire.newLongReference());
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            wire.read(RollFields.cycle).int64(this.cycle, this, (o, i) -> o.cycle = i)
                    .read(RollFields.length).int32(this, (o, i) -> o.length = i)
                    .read(RollFields.format).text(this, (o, i) -> o.format = i)
                    .read(RollFields.timeZone).text(this, (o, i) -> o.zoneId = ZoneId.of(i))
                    .read(RollFields.nextCycle).int64(this.nextCycle, this, (o, i) -> o.nextCycle = i)
                    .read(RollFields.epoc).int64(this, (o, i) -> o.epoc = i)
                    .read(RollFields.nextCycleMetaPosition).int64(this.nextCycleMetaPosition, this, (o, i) -> o.nextCycleMetaPosition = i);
        }

        /**
         * @return an epoc offset as the number of number of milliseconds since January 1, 1970,
         * 00:00:00 GMT
         */
        public long epoc() {
            return this.epoc;
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

        public long nextCycleMetaPosition() {
            return this.nextCycleMetaPosition.getVolatileValue();
        }

        public long nextRollCycle() {
            return this.nextCycle.getVolatileValue();
        }

        public boolean casNextRollCycle(long rollCycle) {
            return this.nextCycle.compareAndSwapValue(-1, rollCycle);
        }
    }
}
