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
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.IntValue;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.WireBounds;
import net.openhft.chronicle.queue.impl.WirePool;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WireConstants;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.function.Function;

import static net.openhft.chronicle.queue.impl.WireConstants.*;

/**
 * TODO:
 * - indexing
 */
class SingleChronicleQueueStore implements WireStore {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(SingleChronicleQueueStore.class,"WireStore");
        ClassAliasPool.CLASS_ALIASES.addAlias(Bounds.class,"Bounds");
        ClassAliasPool.CLASS_ALIASES.addAlias(Indexing.class,"Indexing");
        ClassAliasPool.CLASS_ALIASES.addAlias(Roll.class,"Roll");
    }

    enum MetaDataField implements WireKey {
        bounds,
        indexing,
        roll
    }

    private BytesStore bytesStore;
    private WirePool wirePool;
    private Closeable resourceCleaner;
    private final ThreadLocal<WireBounds> positionPool;
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

    SingleChronicleQueueStore(RollCycle rollCycle) {
        this.positionPool = ThreadLocal.withInitial(() -> new WireBounds());
        this.refCount = ReferenceCounter.onReleased(this::performRelease);
        this.bounds = new Bounds();
        this.roll = new Roll(rollCycle);
        this.indexing = new Indexing();
        this.resourceCleaner = null;
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
        return this.roll.getCycle();
    }

    @Override
    public long lastIndex() {
        return this.indexing.getLastIndex();
    }


    @Override
    public boolean appendRollMeta(long cycle) throws IOException {
        if(roll.casNextRollCycle(cycle)) {
            final WireBounds position = append(
                positionPool.get(),
                true,
                w -> w.write(MetaDataField.roll).int32(cycle)
            );


            roll.setNextCycleMetaPosition(position.lower);

            return true;
        }

        return false;
    }

    /**
     *
     * @param writer
     * @return
     * @throws IOException
     *
     * //TODO: check meta-data for rolling
     */
    @Override
    public long append(@NotNull WriteMarshallable writer) throws IOException {
        final WireBounds wb = append(positionPool.get(), false, writer);

        bounds.setWritePositionIfGreater(wb.upper);
        return indexing.incrementLastIndex();
    }

    /**
     *
     * @param position
     * @param reader
     * @return the new position, 0 if no data -position if roll
     */
    @Override
    public long read(long position, @NotNull ReadMarshallable reader) throws IOException {
        final int spbHeader = bytesStore.readVolatileInt(position);
        if(Wires.isNotInitialized(spbHeader)) {
            return WireConstants.NO_DATA;
        }

        if(Wires.isData(spbHeader) && Wires.isReady(spbHeader)) {
            return Wires.readData(wirePool.acquireForReadAt(position), reader);
        } else if (Wires.isKnownLength(spbHeader)) {
            // In case of meta data, if we are found the "roll" meta, we returns
            // the next cycle (negative)
            final StringBuilder sb = WireConstants.SBP.acquireStringBuilder();
            final ValueIn vi = wirePool.acquireForReadAt(position + 4).read(sb);

            if("roll".contentEquals(sb)) {
                return -vi.int32();
            } else {
                // it it is meta-data and length is know, try a new read
                position += Wires.lengthOf(spbHeader) + SPB_DATA_HEADER_SIZE;
                return read(position, reader);
            }
        }

        return WireConstants.NO_DATA;
    }

    /**
     *
     * @param index
     * @return
     */
    @Override
    public long positionForIndex(long index) {
        long position = readPosition();
        for(long i = 0; i <= index; i++) {
            final int spbHeader = bytesStore.readVolatileInt(position);
            if (Wires.isData(spbHeader) && Wires.isKnownLength(spbHeader)) {
                if(index == i) {
                    return position;
                } else {
                    position += Wires.lengthOf(spbHeader) + SPB_DATA_HEADER_SIZE;
                }
            }
        }

        return -1;
    }

    /**
     * Check if there is room for append.
     *
     * TODO: more accurate space checking
     */
    protected void checkRemainingForAppend() {
        long remaining = bytesStore.writeRemaining();
        if (Wires.exceedsMaxLength(remaining)) {
            throw new IllegalStateException("Length too large: " + remaining);
        }
    }

    @Override
    public void install(
            @NotNull BytesStore store,
            long length,
            boolean created,
            long cycle,
            @NotNull Function<Bytes, Wire> wireSupplier,
            @Nullable Closeable closeable) throws IOException {

        this.bytesStore = store;
        this.wirePool = new WirePool(bytesStore, wireSupplier);

        if(created) {
            this.bounds.setWritePosition(length);
            this.bounds.setReadPosition(length);
            this.roll.setCycle(cycle);
        }
    }

    /**
     *
     * @param writer
     * @return
     * @throws IOException
     */
    protected WireBounds append(WireBounds bounds, boolean meta, @NotNull WriteMarshallable writer)
            throws IOException {

        checkRemainingForAppend();

        long TIMEOUT_MS = 10_000; // 10 seconds.
        long end = System.currentTimeMillis() + TIMEOUT_MS;
        long lastWritePosition = writePosition();

        for (; ;) {
            if(Wires.acquireLock(bytesStore, lastWritePosition)) {
                bounds.lower = lastWritePosition;
                bounds.upper = !meta
                    ? Wires.writeData(wirePool.acquireForWriteAt(lastWritePosition), writer)
                    : Wires.writeMeta(wirePool.acquireForWriteAt(lastWritePosition), writer);

                return bounds;
            } else {
                int spbHeader = bytesStore.readInt(lastWritePosition);
                if (Wires.isKnownLength(spbHeader)) {
                    lastWritePosition += Wires.lengthOf(spbHeader) + SPB_DATA_HEADER_SIZE;
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
