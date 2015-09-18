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
 * Implementation of ChronicleQueueFormat based on a single file.
 *
 * TODO:
 * - rolling
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
        header
    }

    private final int cycle;
    protected final Function<Bytes, Wire> wireSupplier;
    private final SingleChronicleQueueBuilder builder;
    private final File file;
    private final MappedFile mappedFile;
    private final BytesStore bytesStore;
    private final SingleChronicleQueueHeader header;
    private final ThreadLocal<Wire> wireInCache;
    private final ThreadLocal<Wire> wireOutCache;

    SingleChronicleQueueStore(final SingleChronicleQueueBuilder builder, int cycle, String cycleFormat) throws IOException {

        this.builder = builder;
        this.cycle = cycle;
        this.wireSupplier = WireUtil.wireSupplierFor(builder.wireType());
        this.file = new File(this.builder.path(), cycleFormat + ".chronicle");

        if(!this.file.getParentFile().exists()) {
            this.file.mkdirs();
        }

        this.mappedFile = MappedFile.mappedFile(this.file, this.builder.blockSize());
        this.bytesStore = mappedFile.acquireByteStore(SPB_HEADER_BYTE);
        this.header = new SingleChronicleQueueHeader(this.builder);
        this.wireInCache = wireCache(bytesStore::bytesForRead, wireSupplier);
        this.wireOutCache = wireCache(bytesStore::bytesForWrite, wireSupplier);
    }

    long dataPosition() {
        return this.header.getDataPosition();
    }

    long writePosition() {
        return this.header.getWritePosition();
    }

    int cycle() {
        return this.header.getRollCycle();
    }

    long append(@NotNull WriteMarshallable writer) throws IOException {
        checkRemainingForAppend();

        int delay = builder.appendWaitDelay();
        long lastByte = header.getWritePosition();

        for (int i = builder.appendWaitLoops(); i >= 0; i--) {
            if(bytesStore.compareAndSwapInt(lastByte, WireUtil.FREE, WireUtil.BUILDING)) {
                header.setWritePosition(
                    WireUtil.writeDataAt(wireOutCache.get(), lastByte, writer)
                );

                return header.incrementLastIndex();
            } else {
                int header = bytesStore.readInt(lastByte);
                if(WireUtil.isKnownLength(header)) {
                    lastByte += Wires.lengthOf(header) + SPB_DATA_HEADER_SIZE;
                } else {
                    //
                    if(delay > 0) {
                        Jvm.pause(delay);
                    }
                }
            }
        }

        throw new AssertionError("Timeout waiting to append");
    }

    long read(long position, @NotNull ReadMarshallable reader) {
        int header = bytesStore.readVolatileInt(position);
        if(Wires.isData(header)) {
            return WireUtil.readDataAt(wireInCache.get(), position, reader);
        } else if (WireUtil.isKnownLength(header)) {
            // it it is meta-data and length is know, try a new read
            position += Wires.lengthOf(header) + SPB_DATA_HEADER_SIZE;
            return read(position, reader);
        }

        return WireUtil.NO_DATA;
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
            writeMetaAt(
                wireOut(bytesStore.bytesForWrite()),
                SPB_HEADER_BYTE + SPB_HEADER_BYTE_SIZE,
                w -> w.write(MetaDataField.header).typedMarshallable(header)
            );

            // Needed because header.dataPosition, header.writePosition are initially
            // null and initialized when needed. It may be better to initialize
            // them upon header instantiation (?)
            long readPosition = readMetaAt(
                wireIn(bytesStore.bytesForRead()),
                SPB_HEADER_BYTE + SPB_HEADER_BYTE_SIZE,
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

            readMetaAt(
                wireIn(bytesStore.bytesForRead()),
                SPB_HEADER_BYTE + SPB_HEADER_BYTE_SIZE,
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

    // *************************************************************************
    // Wire Helpers
    // *************************************************************************

    protected WireOut wireOut(@NotNull Bytes bytes) throws IOException {
        return this.wireSupplier.apply(bytes);
    }

    protected WireOut wireOut(@NotNull MappedFile file, long offset) throws IOException {
        return wireOut(file.acquireBytesForWrite(offset));
    }

    protected WireIn wireIn(@NotNull Bytes bytes) throws IOException {
        return this.wireSupplier.apply(bytes);
    }

    protected WireIn wireIn(@NotNull MappedFile file, long offset) throws IOException {
        return wireIn(file.acquireBytesForRead(offset));
    }
}
