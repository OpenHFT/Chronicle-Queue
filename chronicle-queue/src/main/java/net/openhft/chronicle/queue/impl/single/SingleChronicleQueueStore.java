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
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import static net.openhft.chronicle.wire.WireUtil.*;

/**
 * TODO:
 * - indexing
 */
class SingleChronicleQueueStore {
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
    private final Function<Bytes, Wire> wireSupplier;
    private final SingleChronicleQueueBuilder builder;
    private final File file;
    private final MappedFile mappedFile;
    private final BytesStore bytesStore;
    private final SingleChronicleQueueHeader header;
    private final WirePool wireInPool;
    private final WirePool wireOutPool;
    private final ThreadLocal<WirePosition> positionPool;

    /**
     *
     * @param builder       the SingleChronicleQueueBuilder
     * @param cycle         the cycle this store refers to
     * @param cycleFormat   the cycle format for folder creation
     *
     * @throws IOException
     */
    SingleChronicleQueueStore(
        final SingleChronicleQueueBuilder builder, int cycle, String cycleFormat) throws IOException {

        this.builder = builder;
        this.cycle = cycle;
        this.wireSupplier = WireUtil.wireSupplierFor(builder.wireType());
        this.file = new File(this.builder.path(), cycleFormat + ".chronicle");

        if(!this.file.getParentFile().exists()) {
            this.file.mkdirs();
        }

        this.mappedFile = MappedFile.mappedFile(this.file, this.builder.blockSize());
        this.bytesStore = mappedFile.acquireByteStore(SPB_HEADER_BYTE);
        this.wireInPool = new WirePool(bytesStore::bytesForRead, wireSupplier);
        this.wireOutPool = new WirePool(bytesStore::bytesForWrite, wireSupplier);
        this.positionPool = ThreadLocal.withInitial(() -> new WirePosition());

        this.header = new SingleChronicleQueueHeader(this.builder);
    }

    long dataPosition() {
        return this.header.getDataPosition();
    }

    long writePosition() {
        return this.header.getWritePosition();
    }

    int cycle() {
        return this.cycle;
    }

    long lastIndex() {
        return this.header.getLastIndex();
    }


    boolean appendRollMeta(int cycle) throws IOException {
        if(header.casNextRollCycle(cycle)) {
            final WirePosition position = append(
                positionPool.get(),
                true,
                w -> w.write(MetaDataField.roll).int32(cycle)
            );


            header.setNextCycleMetaPosition(position.start);

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
    long append(@NotNull WriteMarshallable writer) throws IOException {
        final WirePosition position = append(positionPool.get(), false, writer);

        header.setWritePosition(position.end);
        return header.incrementLastIndex();
    }

    /**
     *
     * @param position
     * @param reader
     * @return the new position, 0 if no data -position if roll
     */
    long read(long position, @NotNull ReadMarshallable reader) throws IOException {
        final int spbHeader = bytesStore.readVolatileInt(position);
        if(spbHeader == WireUtil.NO_DATA) {
            return  WireUtil.NO_DATA;
        }

        if(Wires.isData(spbHeader)) {
            return WireUtil.readData(wireInPool.acquireForReadAt(position), reader);
        } else if (WireUtil.isKnownLength(spbHeader)) {
            // In case of meta data, if we are found the "roll" meta, we returns
            // the next cycle (negative)
            final StringBuilder sb = WireUtil.SBP.acquireStringBuilder();
            final ValueIn vi = wireInPool.acquireForReadAt(position + 4).read(sb);

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
    long positionForIndex(long index) {
        long position = dataPosition();
        for(long i = 0; i <= index; i++) {
            final int spbHeader = bytesStore.readVolatileInt(position);
            if(WireUtil.isData(spbHeader) && WireUtil.isKnownLength(spbHeader)) {
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
    protected SingleChronicleQueueStore buildHeader() throws IOException {
        if(bytesStore.compareAndSwapLong(SPB_HEADER_BYTE, SPB_HEADER_UNSET, SPB_HEADER_BUILDING)) {
            writeMeta(
                wireOutPool.acquireForWriteAt(SPB_HEADER_BYTE + SPB_HEADER_BYTE_SIZE),
                w -> w.write(MetaDataField.header).typedMarshallable(header)
            );

            // Needed because header.dataPosition, header.writePosition are initially
            // null and initialized when needed. It may be better to initialize
            // them upon header instantiation (?)
            long readPosition = readMeta(
                wireInPool.acquireForReadAt(SPB_HEADER_BYTE + SPB_HEADER_BYTE_SIZE),
                w -> w.read().marshallable(header)
            );

            if(WireUtil.NO_DATA == readPosition) {
                throw new AssertionError("Unable to read Header");
            }

            // Set read/write pointer after the header
            header.setDataPosition(readPosition);
            header.setWritePosition(readPosition);
            header.setRollCycle(this.cycle);

            if (!bytesStore.compareAndSwapLong(SPB_HEADER_BYTE, SPB_HEADER_BUILDING, SPB_HEADER_BUILT)) {
                throw new AssertionError("Concurrent writing of the header");
            }
        } else {
            waitForTheHeaderToBeBuilt();

            readMeta(
                wireInPool.acquireForReadAt(SPB_HEADER_BYTE + SPB_HEADER_BYTE_SIZE),
                w -> w.read().marshallable(header)
            );
        }

        return this;
    }

    /**
     * Wait for the header to build.
     *
     * If it exceed the number of attempts defined by builder.headerWaitLoops()
     * each with a timeout of builder.headerWaitDelay() it will throw an AssertionError.
     *
     * @throws IOException
     */
    protected void waitForTheHeaderToBeBuilt() throws IOException {
        for (int i = builder.headerWaitLoops(); i >= 0; i--) {
            long magic = this.bytesStore.readVolatileLong(SPB_HEADER_BYTE);
            if (magic == SPB_HEADER_BUILDING) {
                Jvm.pause(builder.headerWaitDelay());
            } else if (magic == SPB_HEADER_BUILT) {
                return;
            } else {
                throw new AssertionError(
                    "Invalid magic number " + Long.toHexString(magic));
            }
        }

        throw new AssertionError("Timeout waiting to build the file");
    }

    /**
     *
     * @param writer
     * @return
     * @throws IOException
     */
    protected WirePosition append(WirePosition position, boolean meta, @NotNull WriteMarshallable writer) throws IOException {
        checkRemainingForAppend();

        final int delay = builder.appendWaitDelay();
        long lastWritePosition = header.getWritePosition();

        for (int i = builder.appendWaitLoops(); i >= 0; i--) {
            if(bytesStore.compareAndSwapInt(lastWritePosition, WireUtil.FREE, WireUtil.BUILDING)) {
                position.start = lastWritePosition;
                position.end = !meta
                    ? WireUtil.writeData(wireOutPool.acquireForWriteAt(lastWritePosition), writer)
                    : WireUtil.writeMeta(wireOutPool.acquireForWriteAt(lastWritePosition), writer);

                return position;
            } else {
                int spbHeader = bytesStore.readInt(lastWritePosition);
                if(WireUtil.isKnownLength(spbHeader)) {
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
}
