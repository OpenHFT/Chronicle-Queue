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

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.queue.impl.WirePool;
import net.openhft.chronicle.wire.WireUtil;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

import static net.openhft.chronicle.wire.WireUtil.BUILDING;
import static net.openhft.chronicle.wire.WireUtil.HEADER_OFFSET;
import static net.openhft.chronicle.wire.WireUtil.NOT_INITIALIZED;
import static net.openhft.chronicle.wire.WireUtil.NOT_READY;
import static net.openhft.chronicle.wire.WireUtil.SPB_DATA_HEADER_SIZE;
import static net.openhft.chronicle.wire.WireUtil.WireBounds;
import static net.openhft.chronicle.wire.WireUtil.readMeta;
import static net.openhft.chronicle.wire.WireUtil.writeMeta;

/**
 * TODO:
 * - indexing
 */
class SingleChronicleQueueWireStore implements WireStore {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(
            SingleChronicleQueueHeader.class,
            SingleChronicleQueueHeader.CLASS_ALIAS
        );
    }

    enum MetaDataField implements WireKey {
        header,
        roll
    }

    private final int cycle;
    private final SingleChronicleQueueBuilder builder;
    private final File file;
    private final MappedFile mappedFile;
    private final BytesStore bytesStore;
    private final SingleChronicleQueueHeader header;
    private final WirePool wirePool;
    private final ThreadLocal<WireBounds> positionPool;
    private final ReferenceCounter refCount;

    /**
     *
     * @param builder       the SingleChronicleQueueBuilder
     * @param cycle         the cycle this store refers to
     * @param cycleFormat   the cycle format for folder creation
     *
     * @throws IOException
     */
    SingleChronicleQueueWireStore(
        final SingleChronicleQueueBuilder builder, int cycle, String cycleFormat) throws IOException {

        this.builder = builder;
        this.cycle = cycle;
        this.file = new File(this.builder.path(), cycleFormat + ".chronicle");

        if(!this.file.getParentFile().exists()) {
            this.file.mkdirs();
        }

        this.mappedFile = MappedFile.mappedFile(this.file, this.builder.blockSize());
        this.bytesStore = mappedFile.acquireByteStore(HEADER_OFFSET);
        this.wirePool = new WirePool(bytesStore, builder.wireType());
        this.positionPool = ThreadLocal.withInitial(() -> new WireBounds());
        this.refCount = ReferenceCounter.onReleased(this::performRelease);

        this.header = new SingleChronicleQueueHeader(this.builder);
    }

    @Override
    public long readPosition() {
        return this.header.getReadPosition();
    }

    @Override
    public long writePosition() {
        return this.header.getWritePosition();
    }

    @Override
    public int cycle() {
        return this.cycle;
    }

    @Override
    public long lastIndex() {
        return this.header.getLastIndex();
    }


    @Override
    public boolean appendRollMeta(int cycle) throws IOException {
        if(header.casNextRollCycle(cycle)) {
            final WireBounds position = append(
                positionPool.get(),
                true,
                w -> w.write(MetaDataField.roll).int32(cycle)
            );


            header.setNextCycleMetaPosition(position.lower);

            return true;
        }

        return false;
    }

    /**
     *
     * @param writer
     * @return
     * @throws IOException
     */
    @Override
    public long append(@NotNull WriteMarshallable writer) throws IOException {
        final WireBounds bounds = append(positionPool.get(), false, writer);

        header.setWritePosition(bounds.upper);
        return header.incrementLastIndex();
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
        if(spbHeader == WireUtil.NO_DATA) {
            return WireUtil.NO_DATA;
        }

        if(Wires.isData(spbHeader) && Wires.isReady(spbHeader)) {
            return WireUtil.readData(wirePool.acquireForReadAt(position), reader);
        } else if (Wires.isKnownLength(spbHeader)) {
            // In case of meta data, if we are found the "roll" meta, we returns
            // the next cycle (negative)
            final StringBuilder sb = WireUtil.SBP.acquireStringBuilder();
            final ValueIn vi = wirePool.acquireForReadAt(position + 4).read(sb);

            if("roll".contentEquals(sb)) {
                return -vi.int32();
            } else {
                // it it is meta-data and length is know, try a new read
                position += Wires.lengthOf(spbHeader) + SPB_DATA_HEADER_SIZE;
                return read(position, reader);
            }
        }

        return WireUtil.NO_DATA;
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
        if (remaining > WireUtil.LENGTH_MASK) {
            throw new IllegalStateException("Length too large: " + remaining);
        }
    }

    /**
     * Build the header (@see SingleChronicleQueueHeader)
     *
     * @throws IOException
     */
    protected SingleChronicleQueueWireStore buildHeader() throws IOException {
        if(bytesStore.compareAndSwapLong(HEADER_OFFSET, NOT_INITIALIZED, NOT_READY)) {
            writeMeta(
                wirePool.acquireForWriteAt(HEADER_OFFSET),
                w -> w.write(MetaDataField.header).typedMarshallable(header)
            );

            // Needed because header.readPosition, header.writePosition are initially
            // null and initialized when needed. It may be better to initialize
            // them upon header instantiation (?)
            long readPosition = readMeta(
                wirePool.acquireForReadAt(HEADER_OFFSET),
                w -> w.read().marshallable(header)
            );

            if(WireUtil.NO_DATA == readPosition) {
                throw new AssertionError("Unable to read Header");
            }

            // Set read/write pointer after the header
            header.setReadPosition(readPosition);
            header.setWritePosition(readPosition);
            header.setRollCycle(this.cycle);
        } else {
            WireUtil.waitForWireToBeReady(
                this.bytesStore,
                HEADER_OFFSET,
                builder.headerWaitLoops(),
                builder.headerWaitDelay());

            readMeta(
                wirePool.acquireForReadAt(HEADER_OFFSET),
                w -> w.read().marshallable(header)
            );
        }

        return this;
    }

    /**
     *
     * @param writer
     * @return
     * @throws IOException
     */
    protected WireBounds append(WireBounds bounds, boolean meta, @NotNull WriteMarshallable writer) throws IOException {
        checkRemainingForAppend();

        final int delay = builder.appendWaitDelay();
        long lastWritePosition = header.getWritePosition();

        for (int i = builder.appendWaitLoops(); i >= 0; i--) {
            if(bytesStore.compareAndSwapInt(lastWritePosition, NOT_INITIALIZED, BUILDING)) {
                bounds.lower = lastWritePosition;
                bounds.upper = !meta
                    ? WireUtil.writeData(wirePool.acquireForWriteAt(lastWritePosition), writer)
                    : WireUtil.writeMeta(wirePool.acquireForWriteAt(lastWritePosition), writer);

                return bounds;
            } else {
                int spbHeader = bytesStore.readInt(lastWritePosition);
                if (Wires.isKnownLength(spbHeader)) {
                    lastWritePosition += Wires.lengthOf(spbHeader) + SPB_DATA_HEADER_SIZE;
                } else {
                    // TODO: wait strategy
                    if(delay > 0) {
                        Jvm.pause(delay);
                    }
                }
            }
        }

        throw new AssertionError("Timeout waiting to append");
    }

    private synchronized void performRelease() {
        this.mappedFile.close();
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
}
