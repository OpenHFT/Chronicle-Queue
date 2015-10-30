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
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.ReadContext;
import net.openhft.chronicle.queue.impl.WireConstants;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WriteContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.function.Function;

import static net.openhft.chronicle.queue.impl.WireConstants.SPB_DATA_HEADER_SIZE;

/**
 * TODO:
 * - indexing
 */
class SingleChronicleQueueStore implements WireStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleChronicleQueueStore.class);

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Bounds.class,"Bounds");
        ClassAliasPool.CLASS_ALIASES.addAlias(Indexing.class,"Indexing");
        ClassAliasPool.CLASS_ALIASES.addAlias(Roll.class,"Roll");
    }

    enum MetaDataField implements WireKey {
        bounds,
        indexing,
        roll
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
        this(null);
    }

    SingleChronicleQueueStore(@Nullable RollCycle rollCycle) {
        this.refCount = ReferenceCounter.onReleased(this::performRelease);
        this.bounds = new Bounds();
        this.roll = new Roll(rollCycle);
        this.indexing = new Indexing();
        this.resourceCleaner = null;
        this.builder = null;
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
    public void acquireBytesAtReadPositionForWrite(@NotNull VanillaBytes<?> bytes) throws IOException {
        this.mappedFile.acquireBytesForWrite(readPosition(), bytes);
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
        if(roll.casNextRollCycle(cycle)) {
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
    public boolean moveToIndex(@NotNull ReadContext context, long index){
        long position = readPosition();
        try {
            for (long i = 0; i <= index;) {
                if(context.bytes.readRemaining() == 0 || position > context.bytes.safeLimit()) {
                    mappedFile.acquireBytesForRead(position, context.bytes);
                }

                final int spbHeader = context.bytes.readVolatileInt(position);
                if(Wires.isReady(spbHeader)) {
                    if(Wires.isData(spbHeader)) {
                        if (index == i) {
                            return true;
                        }

                        i++;
                    }

                    context.bytes.readSkip(Wires.lengthOf(spbHeader) + SPB_DATA_HEADER_SIZE);
                } else {
                    return false;
                }
            }
        } catch(IOException e) {
            throw new IllegalStateException(e);
        }

        return false;
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

        this.builder = (SingleChronicleQueueBuilder)builder;
        this.mappedFile = mappedFile;

        if(created) {
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
            if(this.resourceCleaner != null) {
                this.resourceCleaner.close();
            }
        } catch(IOException e) {
            //TODO
        }
    }

    private int toIntU30(long len) {
        return Wires.toIntU30(len,"Document length %,d out of 30-bit int range.");
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
        if(context.bytes.readRemaining() == 0) {
            mappedFile.acquireBytesForRead(position, context.bytes);
        }

        final int spbHeader = context.bytes.readVolatileInt(position);
        if(!Wires.isNotInitialized(spbHeader) && Wires.isReady(spbHeader)) {
            int len = Wires.lengthOf(spbHeader);
            if(Wires.isData(spbHeader)) {
                return reader.read(context, len, marshaller);
            } else {
                // In case of meta data, if we are found the "roll" meta, we returns
                // the next cycle (negative)
                final StringBuilder sb = Wires.acquireStringBuilder();
                final ValueIn vi = context.wire(position + SPB_DATA_HEADER_SIZE, builder.blockSize()).read(sb);

                if("roll".contentEquals(sb)) {
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
        return indexing.incrementLastIndex();
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
        return indexing.incrementLastIndex();
    }

    private long writeBytes(
            @NotNull WriteContext context,
            long position,
            int size,
            @NotNull final Bytes bytes) throws IOException {

        context.bytes.writeSkip(4);
        context.bytes.write(bytes);
        context.bytes.compareAndSwapInt(position, size | Wires.NOT_READY, size);

        return indexing.incrementLastIndex();
    }

    private <T> long write(
            @NotNull WriteContext context,
            int size,
            @NotNull Writer<T> writer,
            @NotNull T marshaller) throws IOException {

        final long end = System.currentTimeMillis() + builder.appendTimeout();
        long position = writePosition();

        for (; ;) {
            if (position > context.bytes.safeLimit()) {
                mappedFile.acquireBytesForWrite(position, context.bytes);
            }

            if(context.bytes.compareAndSwapInt(position, Wires.NOT_INITIALIZED, Wires.NOT_READY | size)) {
                return writer.write(context, position, size, marshaller);
            } else {
                int spbHeader = context.bytes.readInt(position);
                if (Wires.isKnownLength(spbHeader)) {
                    position += Wires.lengthOf(spbHeader) + SPB_DATA_HEADER_SIZE;
                } else {
                    // TODO: wait strategy
                    if(System.currentTimeMillis() > end) {
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
            for(; ;) {
                long wp = writePosition();
                if(writePosition > wp) {
                    if(this.writePosition.compareAndSwapValue(wp, writePosition)) {
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

    class Indexing implements Marshallable {
        private int indexCount;
        private int indexSpacing;
        private LongValue index2Index;
        private LongValue lastIndex;

        Indexing() {
            this.indexCount = 128 << 10;
            this.indexSpacing = 64;
            this.index2Index = null;
            this.lastIndex = null;
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
