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
import net.openhft.chronicle.queue.impl.ReadContext;
import net.openhft.chronicle.queue.impl.WireConstants;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WriteContext;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.function.Function;

import static net.openhft.chronicle.queue.impl.WireConstants.SPB_DATA_HEADER_SIZE;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.IndexOffset.toAddress0;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.IndexOffset.toAddress1;
import static net.openhft.chronicle.wire.Wires.NOT_INITIALIZED;
import static net.openhft.chronicle.wire.Wires.NOT_READY;


class SingleChronicleQueueStore implements WireStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleChronicleQueueStore.class);

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Bounds.class, "Bounds");
        ClassAliasPool.CLASS_ALIASES.addAlias(Indexing.class, "Indexing");
        ClassAliasPool.CLASS_ALIASES.addAlias(Roll.class, "Roll");
    }

    enum MetaDataField implements WireKey {
        bounds,
        indexing,
        roll
    }

    private static ThreadLocal<LongArrayValues> newLongArrayValuesPool(WireType wireType) {

        if (wireType == WireType.TEXT)
            return ThreadLocal.withInitial(TextLongArrayReference::new);
        if (wireType == WireType.BINARY)
            return ThreadLocal.withInitial(BinaryLongArrayReference::new);
        else
            throw new IllegalStateException("todo, unsupported type=" + wireType);
    }

    private MappedFile mappedFile;
    private Closeable resourceCleaner;
    private SingleChronicleQueueBuilder builder;

    private final ReferenceCounter refCount;

    private final Bounds bounds;

    private final Indexing indexing;
    private final Roll roll;

    /**
     * Default constructor needed for self boot-strapping
     */
    SingleChronicleQueueStore() {
        this(null, WireType.BINARY);
    }

    SingleChronicleQueueStore(@Nullable RollCycle rollCycle, final WireType wireType) {
        this.refCount = ReferenceCounter.onReleased(this::performRelease);
        this.bounds = new Bounds();
        this.roll = new Roll(rollCycle);
        this.resourceCleaner = null;
        this.builder = null;
        this.indexing = new Indexing(wireType);
    }

    @Override
    public long readPosition() {
        return this.bounds.getReadPosition();
    }

    @Override
    public void acquireBytesAtReadPositionForRead(@NotNull VanillaBytes<?> bytes) throws IOException {
        this.mappedFile.acquireBytesForRead(readPosition(), bytes);
    }


    @Override
    public long writePosition() {
        return this.bounds.getWritePosition();
    }

    @Override
    public void acquireBytesAtWritePositionForRead(@NotNull VanillaBytes<?> bytes) throws IOException {
        this.mappedFile.acquireBytesForRead(writePosition(), bytes);
    }

    @Override
    public void acquireBytesAtWritePositionForWrite(@NotNull VanillaBytes<?> bytes) throws IOException {
        this.mappedFile.acquireBytesForWrite(writePosition(), bytes);
    }

    @Override
    public long cycle() {
        return this.roll.getCycle();
    }

    @Override
    public long lastIndex() {
        return this.indexing.getLastIndex();
    }

    @Override
    public long append(@NotNull WriteContext context, @NotNull final WriteMarshallable marshallable) throws IOException {
        return write(context, Wires.UNKNOWN_LENGTH, this::writeWireMarshallable, marshallable);
    }

    @Override
    public long append(@NotNull WriteContext context, @NotNull final WriteBytesMarshallable marshallable) throws IOException {
        return write(context, Wires.UNKNOWN_LENGTH, this::writeBytesMarshallable, marshallable);
    }

    @Override
    public long append(@NotNull WriteContext context, @NotNull final Bytes bytes) throws IOException {
        return write(context, toIntU30(bytes.length()), this::writeBytes, bytes);
    }

    @Override
    public long read(@NotNull ReadContext context, @NotNull ReadMarshallable reader) throws IOException {
        return read(context, this::readWireMarshallable, reader);
    }

    @Override
    public long read(@NotNull ReadContext context, @NotNull ReadBytesMarshallable reader) throws IOException {
        return read(context, this::readBytesMarshallable, reader);
    }

    @Override
    public boolean appendRollMeta(@NotNull WriteContext context, long cycle) throws IOException {
        if (roll.casNextRollCycle(cycle)) {
            final WriteMarshallable marshallable = x -> x.write(MetaDataField.roll).int32(cycle);

            write(
                    context,
                    Wires.UNKNOWN_LENGTH,
                    (WriteContext ctx, long position, int size, WriteMarshallable w) -> {
                        Wires.writeMeta(context.wire, w);
                        roll.setNextCycleMetaPosition(position);
                        return WireConstants.NO_INDEX;
                    },
                    marshallable
            );

            return true;
        }

        return false;
    }

    @Override
    public boolean moveToIndex(@NotNull ReadContext context, long index) {
        return indexing.moveToIndex(context, index);
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
            @NotNull MappedFile mappedFile,
            long length,
            boolean created,
            long cycle,
            ChronicleQueueBuilder builder,
            @NotNull Function<Bytes, Wire> wireSupplier,
            @Nullable Closeable closeable) throws IOException {

        this.builder = (SingleChronicleQueueBuilder) builder;
        this.mappedFile = mappedFile;
        this.indexing.setMappedFile(mappedFile);
        if (created) {
            this.bounds.setWritePosition(length);
            this.bounds.setReadPosition(length);
            this.roll.setCycle(cycle);
        }
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

    @FunctionalInterface
    private interface Reader<T> {
        long read(@NotNull ReadContext context, int len, @NotNull T reader) throws IOException;
    }

    private long readWireMarshallable(
            @NotNull ReadContext context,
            int len,
            @NotNull ReadMarshallable marshaller) {

        context.bytes.readSkip(SPB_DATA_HEADER_SIZE);
        return readWire(context.wire, len, marshaller);
    }

    private long readBytesMarshallable(
            @NotNull ReadContext context,
            int len,
            @NotNull ReadBytesMarshallable marshaller) {

        context.bytes.readSkip(SPB_DATA_HEADER_SIZE);
        final long readp = context.bytes.readPosition();
        final long readl = context.bytes.readLimit();

        marshaller.readMarshallable(context.bytes);
        context.bytes.readPosition(readp + len);
        context.bytes.readLimit(readl);

        return readp + len;
    }

    private <T> long read(
            @NotNull ReadContext context,
            @NotNull Reader<T> reader,
            @NotNull T marshaller) throws IOException {

        long position = context.bytes.readPosition();
        if (context.bytes.readRemaining() == 0) {
            mappedFile.acquireBytesForRead(position, context.bytes);
        }

        final int spbHeader = context.bytes.readVolatileInt(position);
        if (!Wires.isNotInitialized(spbHeader) && Wires.isReady(spbHeader)) {
            int len = Wires.lengthOf(spbHeader);
            if (Wires.isData(spbHeader)) {
                return reader.read(context, len, marshaller);
            } else {
                // In case of meta data, if we are found the "roll" meta, we returns
                // the next cycle (negative)
                final StringBuilder sb = Wires.acquireStringBuilder();
                final ValueIn vi = context.wire(position + SPB_DATA_HEADER_SIZE, builder.blockSize()).read(sb);

                if ("index".contentEquals(sb)) {
                    return read(context, reader, marshaller);
                } else if ("roll".contentEquals(sb)) {
                    return -vi.int32();
                } else {
                    context.bytes.readPosition(position + len + SPB_DATA_HEADER_SIZE);
                    return read(context, reader, marshaller);
                }
            }
        }

        return WireConstants.NO_DATA;
    }

    //TODO : maybe move to wire
    @ForceInline
    private long readWire(@NotNull WireIn wireIn, long size, @NotNull ReadMarshallable dataConsumer) {
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

    // *************************************************************************
    // Utilities :: Write
    // *************************************************************************

    @FunctionalInterface
    private interface Writer<T> {
        long write(@NotNull WriteContext context, long position, int size, @NotNull T writer) throws IOException;
    }

    private long writeWireMarshallable(
            @NotNull WriteContext context,
            long position,
            int size,
            @NotNull final WriteMarshallable marshallable) throws IOException {

        bounds.setWritePositionIfGreater(Wires.writeData(context.wire, marshallable));
        final long index = indexing.incrementLastIndex();
        indexing.storeIndexLocation(context, position, index);
        return index;
    }

    private long writeBytesMarshallable(
            @NotNull WriteContext context,
            long position,
            int size,
            @NotNull final WriteBytesMarshallable marshallable) throws IOException {

        context.bytes.writeSkip(SPB_DATA_HEADER_SIZE);

        marshallable.writeMarshallable(context.bytes);
        context.bytes.compareAndSwapInt(
                position,
                Wires.NOT_READY,
                toIntU30(context.bytes.writePosition() - position - SPB_DATA_HEADER_SIZE)
        );

        bounds.setWritePositionIfGreater(position);
        final long index = indexing.incrementLastIndex();
        indexing.storeIndexLocation(context, position, index);
        return index;
    }

    private long writeBytes(
            @NotNull WriteContext context,
            long position,
            int size,
            @NotNull final Bytes bytes) throws IOException {

        context.bytes.writeSkip(4);
        context.bytes.write(bytes);
        context.bytes.compareAndSwapInt(position, size | Wires.NOT_READY, size);

        final long index = indexing.incrementLastIndex();
        indexing.storeIndexLocation(context, position, index);
        return index;
    }


    private long writeIndexBytes(
            @NotNull WriteContext context,
            long position,
            int size,
            @NotNull final Bytes bytes) throws IOException {

        context.bytes.writeSkip(4);
        context.bytes.write(bytes);
        context.bytes.compareAndSwapInt(position, size | Wires.NOT_READY, size);
        return 0;
    }

    private <T> long write(
            @NotNull WriteContext context,
            int size,
            @NotNull Writer<T> writer,
            @NotNull T marshaller) throws IOException {

        final long end = System.currentTimeMillis() + builder.appendTimeout();
        long position = writePosition();

        for (; ; ) {
            if (position > context.bytes.safeLimit()) {
                mappedFile.acquireBytesForWrite(position, context.bytes);
            }

            if (context.bytes.compareAndSwapInt(position, Wires.NOT_INITIALIZED, Wires.NOT_READY | size)) {
                return writer.write(context, position, size, marshaller);
            } else {
                int spbHeader = context.bytes.readInt(position);
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

    // *************************************************************************
    // Marshallable
    // *************************************************************************

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(MetaDataField.bounds).typedMarshallable(this.bounds)
                .write(MetaDataField.indexing).typedMarshallable(this.indexing)
                .write(MetaDataField.roll).typedMarshallable(this.roll);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        wire.read(MetaDataField.bounds).marshallable(this.bounds)
                .read(MetaDataField.indexing).marshallable(this.indexing)
                .read(MetaDataField.roll).marshallable(this.roll);
    }

    // *************************************************************************
    //
    // *************************************************************************

    enum BoundsField implements WireKey {
        writePosition,
        readPosition,
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

    enum IndexingFields implements WireKey {
        indexCount, indexSpacing, index2Index, lastIndex
    }

    public static final long NUMBER_OF_ENTRIES_IN_EACH_INDEX = 1 << 17;

    class Indexing implements Marshallable {
        private final WireType wireType;
        private int indexCount;
        private int indexSpacing;
        private LongValue index2Index;
        private LongValue lastIndex;
        private final Wire templateIndex;

        private ThreadLocal<LongArrayValues> longArray;
        private MappedFile mappedFile;

        /**
         * @param wireType
         */
        Indexing(@NotNull WireType wireType) {
            this.indexCount = 128 << 10;
            this.indexSpacing = 64;
            this.index2Index = null;
            this.lastIndex = null;

            final Bytes b = Bytes.elasticByteBuffer();
            templateIndex = wireType.apply(b);
            templateIndex.writeDocument(true, w -> w.write(() -> "index")
                    .int64array(NUMBER_OF_ENTRIES_IN_EACH_INDEX));

            this.wireType = wireType;
            longArray = newLongArrayValuesPool(wireType);
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(IndexingFields.indexCount).int32(indexCount)
                    .write(IndexingFields.indexSpacing).int32(indexSpacing)
                    .write(IndexingFields.index2Index).int64forBinding(0L, index2Index = wire.newLongReference())
                    .write(IndexingFields.lastIndex).int64forBinding(-1L, lastIndex = wire.newLongReference());
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            wire.read(IndexingFields.indexCount).int32(this, (o, i) -> o.indexCount = i)
                    .read(IndexingFields.indexSpacing).int32(this, (o, i) -> o.indexSpacing = i)
                    .read(IndexingFields.index2Index).int64(this.index2Index, this, (o, i) -> o.index2Index = i)
                    .read(IndexingFields.lastIndex).int64(this.lastIndex, this, (o, i) -> o.lastIndex = i);
        }

        public long incrementLastIndex() {
            return this.lastIndex.addAtomicValue(1);
        }

        public long getLastIndex() {
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
        long indexToIndex(@Nullable final WriteContext writeContext) {
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
        public void storeIndexLocation(WriteContext context,
                                       final long address,
                                       final long index) throws IOException {

            if (index % 64 != 0 || index == 0)
                return;

            final LongArrayValues array = this.longArray.get();
            final long indexToIndex0 = indexToIndex(context);

            try {

                final Bytes bytes0 = this.mappedFile.acquireBytesForRead(indexToIndex0);
                final Wire w = wireType.apply(bytes0);


                w.readDocument(d -> {

                    final LongArrayValues primaryIndex = array(w, array);

                    try {

                        final long primaryOffset = toAddress0(index);
                        long secondaryAddress = primaryIndex.getValueAt(primaryOffset);
                        if (secondaryAddress == Wires.NOT_INITIALIZED) {
                            secondaryAddress = newIndex(context);
                            primaryIndex.setValueAt(primaryOffset, secondaryAddress);
                        }

                        final Bytes bytes = mappedFile.acquireBytesForRead(secondaryAddress);
                        wireType.apply(bytes).readDocument(document -> {
                            final LongArrayValues array1 = array(document, array);
                            array1.setValueAt(toAddress1(index), address);
                        }, null);

                    } catch (Exception e) {
                        e.printStackTrace();
                        throw Jvm.rethrow(e);
                    }
                }, null);


            } catch (Throwable e) {
                e.printStackTrace();
                throw Jvm.rethrow(e);
            }

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
        long newIndex(WriteContext writeContext) {

            try {
                final long start = writeContext.bytes.writePosition() + SPB_DATA_HEADER_SIZE;
                final Bytes<?> bytes = templateIndex.bytes();
                write(writeContext, toIntU30((long) bytes.length()) | Wires.META_DATA, this::writeIndexBytes, bytes);
                return start;
            } catch (Throwable e) {
                throw Jvm.rethrow(e);
            }

        }

        private long writeIndexBytes(
                @NotNull WriteContext context,
                long position,
                int size,
                @NotNull final Bytes bytes) throws IOException {

            context.bytes.writeSkip(4);
            context.bytes.write(bytes);
            context.bytes.compareAndSwapInt(position, size | Wires.NOT_READY, size);

            final long index = indexing.incrementLastIndex();
            indexing.storeIndexLocation(context, position, index);

            return index;
        }


        void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }


        private LongArrayValues values;

        private long readIndexAt(long index2Index, long offset) throws IOException {

            final Bytes bytes0 = this.mappedFile.acquireBytesForRead(index2Index);
            final Wire w = this.wireType.apply(bytes0);

            final long[] result = new long[1];

            w.readDocument(wireIn -> {
                try {
                    wireIn.read(() -> "index").int64array(values, this, (o, v) -> o.values = v);
                    final long index = values.getVolatileValueAt(offset);
                    result[0] = index;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, null);

            return result[0];
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
        public boolean moveToIndex(ReadContext context, final long targetIndex) {


            long index2index = this.index2Index.getVolatileValue();


            try {

                long address1 = readIndexAt(index2index, toAddress0(targetIndex));
                long fromAddress;

                if (address1 != 0) {

                    final long offset = toAddress1(targetIndex);
                    fromAddress = readIndexAt(address1, offset);

                    if (fromAddress != 0) {
                        ///    context.bytes.readPosition(address2);
                        long startIndex = ((targetIndex / 64L)) * 64L;

                        if (targetIndex == startIndex) {
                            readPosition(context, fromAddress);
                            return true;
                        }

                        return linearScan(context, targetIndex, startIndex, fromAddress);

                    } else {

                        // scan back in secondary index till we find any result then we will
                        // linear scan from their
                        for (long startIndex = offset; startIndex >= 0; startIndex--) {
                            fromAddress = readIndexAt(address1, toAddress1(targetIndex));
                            if (fromAddress != 0) {
                                return linearScan(context, targetIndex, startIndex, fromAddress);
                            }
                        }

                        return linearScan(context, targetIndex, 0, fromAddress);
                    }

                } else {
                    throw new UnsupportedOperationException("todo");
                }


            } catch (Exception e) {
                e.printStackTrace();
            }

            return false;
        }

        private void readPosition(ReadContext context, long position) throws IOException {
            if (context.bytes.readRemaining() == 0 || position > context.bytes.safeLimit()) {
                mappedFile.acquireBytesForRead(position, context.bytes);
            } else {
                context.bytes.readPosition(position);
            }
        }

        /**
         * moves the context to the index of {@code toIndex} by doing a linear scans form a {@code
         * fromKnownIndex} at  {@code knownAddress}
         *
         * note meta data is skipped and does not count to the indexes
         *
         * @param context        if successful, moves the context to an address relating to the
         *                       index {@code toIndex }
         * @param toIndex        the index that we wish to move the context to
         * @param fromKnownIndex a know index ( used as a starting point )
         * @param knownAddress   a know address ( used as a starting point )
         * @return {@code true} if successful
         * @see net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore.Indexing#moveToIndex
         */
        private boolean linearScan(ReadContext context, long toIndex, long fromKnownIndex, long knownAddress) {

            long position = knownAddress;
            try {
                for (long i = fromKnownIndex; i <= toIndex; ) {

                    if (context.bytes.readRemaining() == 0 || position > context.bytes.safeLimit()) {
                        mappedFile.acquireBytesForRead(position, context.bytes);
                    }

                    final int spbHeader = context.bytes.readVolatileInt(position);
                    if (Wires.isReady(spbHeader)) {
                        if (Wires.isData(spbHeader)) {
                            if (toIndex == i) {
                                return true;
                            }

                            i++;
                        }

                        // todo
                        //  if (i % 64 == 0)
                        //    storeIndexLocation(context,context.bytes.readPosition(),i);

                        final int len = Wires.lengthOf(spbHeader);
                        context.bytes.readSkip(len + SPB_DATA_HEADER_SIZE);
                        position = context.bytes.readPosition();
                    } else {
                        return false;
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

            return false;
        }


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
        cycle, length, format, timeZone, nextCycle, nextCycleMetaPosition
    }

    class Roll implements Marshallable {
        private int length;
        private String format;
        private ZoneId zoneId;
        private LongValue cycle;
        private LongValue nextCycle;
        private LongValue nextCycleMetaPosition;

        Roll(RollCycle rollCycle) {
            this.length = rollCycle != null ? rollCycle.length() : -1;
            this.format = rollCycle != null ? rollCycle.format() : null;
            this.zoneId = rollCycle != null ? rollCycle.zone() : null;

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
                    .write(RollFields.nextCycleMetaPosition).int64forBinding(-1, nextCycleMetaPosition = wire.newLongReference());
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) {
            wire.read(RollFields.cycle).int64(this.cycle, this, (o, i) -> o.cycle = i)
                    .read(RollFields.length).int32(this, (o, i) -> o.length = i)
                    .read(RollFields.format).text(this, (o, i) -> o.format = i)
                    .read(RollFields.timeZone).text(this, (o, i) -> o.zoneId = ZoneId.of(i))
                    .read(RollFields.nextCycle).int64(this.nextCycle, this, (o, i) -> o.nextCycle = i)
                    .read(RollFields.nextCycleMetaPosition).int64(this.nextCycleMetaPosition, this, (o, i) -> o.nextCycleMetaPosition = i);
        }

        public long getCycle() {
            return this.cycle.getVolatileValue();
        }

        public Roll setCycle(long rollCycle) {
            this.cycle.setOrderedValue(rollCycle);
            return this;
        }

        public Roll setNextCycleMetaPosition(long position) {
            this.nextCycleMetaPosition.setOrderedValue(position);
            return this;
        }

        public long getNextCycleMetaPosition() {
            return this.nextCycleMetaPosition.getVolatileValue();
        }

        public long getNextRollCycle() {
            return this.nextCycle.getVolatileValue();
        }

        public boolean casNextRollCycle(long rollCycle) {
            return this.nextCycle.compareAndSwapValue(-1, rollCycle);
        }
    }
}
