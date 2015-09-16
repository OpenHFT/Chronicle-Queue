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
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueueFormat;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

import static net.openhft.chronicle.wire.WireUtil.*;

/**
 * Implementation of ChronicleQueueFormat based on a single file.
 *
 * TODO:
 * - rolling
 * - indexing
 */
class SingleChronicleQueueFormat extends AbstractChronicleQueueFormat {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(
            SingleChronicleQueueHeader.class,
            SingleChronicleQueueHeader.CLASS_ALIAS
        );
    }

    enum MetaDataField implements WireKey {
        header
    }

    private final SingleChronicleQueueBuilder builder;
    private final MappedFile mappedFile;
    private final BytesStore bytesStore;
    private final SingleChronicleQueueHeader header;
    private final ThreadLocal<Wire> wireInCache;
    private final ThreadLocal<Wire> wireOutCache;

    SingleChronicleQueueFormat(final SingleChronicleQueueBuilder builder) throws IOException {
        super(builder.wireType());

        this.builder = builder;
        this.mappedFile = MappedFile.mappedFile(this.builder.path(), this.builder.blockSize());
        this.bytesStore = mappedFile.acquireByteStore(SPB_HEADER_BYTE);
        this.header = new SingleChronicleQueueHeader();
        this.wireInCache = wireCache(bytesStore::bytesForRead, wireSupplier());
        this.wireOutCache = wireCache(bytesStore::bytesForWrite, wireSupplier());
    }

    long dataPosition() {
        return this.header.getDataPosition();
    }

    long writePosition() {
        return this.header.getWritePosition();
    }

    @Override
    public long append(@NotNull WriteMarshallable writer) throws IOException {
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

    @Override
    public long read(long position, @NotNull ReadMarshallable reader) {
        return WireUtil.readDataAt(wireInCache.get(), position, reader);
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
    protected SingleChronicleQueueFormat buildHeader() throws IOException {
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

    /**
     *
     * @param builder
     * @return
     * @throws IOException
     */
    public static SingleChronicleQueueFormat from(
        final SingleChronicleQueueBuilder builder) throws IOException {

        return new SingleChronicleQueueFormat(builder).buildHeader();
    }
}
