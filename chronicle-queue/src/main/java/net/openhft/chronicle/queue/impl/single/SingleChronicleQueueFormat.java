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
import net.openhft.chronicle.queue.impl.AbstractChronicleQueueFormat;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireUtil;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.wire.WireUtil.*;

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
    private final BytesStore mappedStore;
    private final SingleChronicleQueueHeader header;
    private final ThreadLocal<Wire> wireInCache;
    private final ThreadLocal<Wire> wireOutCache;

    SingleChronicleQueueFormat(final SingleChronicleQueueBuilder builder) throws IOException {
        super(builder.wireType());

        this.builder = builder;
        this.mappedFile = MappedFile.mappedFile(this.builder.path(), this.builder.blockSize());
        this.mappedStore = mappedFile.acquireByteStore(SPB_HEADER_BYTE);
        this.header = new SingleChronicleQueueHeader();
        this.wireInCache = wireCache(mappedStore::bytesForRead, wireSupplier());
        this.wireOutCache = wireCache(mappedStore::bytesForWrite, wireSupplier());
    }

    long dataPosition() {
        return this.header.getDataPosition();
    }

    @Override
    public long append(@NotNull WriteMarshallable writer) throws IOException {
        checkRemainingForAppend();

        for (long lastByte = header.getWritePosition(); ; ) {
            if(mappedStore.compareAndSwapInt(lastByte, WireUtil.FREE, WireUtil.BUILDING)) {
                final WireOut wo = wireOutCache.get();
                final Bytes wb = wo.bytes();

                wb.writePosition(lastByte);

                WireUtil.writeData(wo, writer);
                header.setWritePosition(wb.writePosition());

                return header.incrementLastIndex();
            } else {
                int header = mappedStore.readInt(lastByte);
                if(WireUtil.isKnownLength(header)) {
                    lastByte += Wires.lengthOf(header) + SPB_DATA_HEADER_SIZE;
                } else {
                    // TODO: need to implement wait (strategy and timeout)
                }
            }
        }
    }

    @Override
    public boolean read(@NotNull AtomicLong position, @NotNull ReadMarshallable reader) {
        final WireIn wi = wireInCache.get();
        final Bytes wb = wi.bytes();

        wb.readPosition(position.get());
        boolean read =  WireUtil.readData(wi, reader);
        position.set(wb.readPosition());

        return read;
    }

    protected void checkRemainingForAppend() {
        long remaining = this.mappedStore.writeRemaining();
        if (remaining > WireUtil.LENGTH_MASK) {
            throw new IllegalStateException("Length too large: " + remaining);
        }
    }

    protected void buildHeader() throws IOException {
        final Bytes rb = this.mappedStore.bytesForRead();
        rb.readPosition(SPB_HEADER_BYTE_SIZE);

        final Bytes wb = this.mappedStore.bytesForWrite();
        wb.writePosition(SPB_HEADER_BYTE_SIZE);

        if(this.mappedStore.compareAndSwapLong(SPB_HEADER_BYTE, SPB_HEADER_UNSET, SPB_HEADER_BUILDING)) {
            writeMeta(
                wireOut(wb),
                w -> w.write(MetaDataField.header).typedMarshallable(this.header)
            );

            readMeta(
                wireIn(rb),
                w -> w.read().marshallable(this.header)
            );

            header.setDataPosition(wb.writePosition());
            header.setWritePosition(wb.writePosition());

            if (!this.mappedStore.compareAndSwapLong(SPB_HEADER_BYTE, SPB_HEADER_BUILDING, SPB_HEADER_BUILT)) {
                throw new AssertionError("Concurrent writing of the header");
            }
        } else {
            waitForTheHeaderToBeBuilt();

            readMeta(
                wireIn(rb),
                w -> w.read().marshallable(header)
            );
        }
    }

    protected void waitForTheHeaderToBeBuilt() throws IOException {
        for (int i = 0; i < 1000; i++) {
            long magic = this.mappedStore.readVolatileLong(SPB_HEADER_BYTE);
            if (magic == SPB_HEADER_BUILDING) {
                Jvm.pause(10);
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
        SingleChronicleQueueFormat format = new SingleChronicleQueueFormat(builder);
        format.buildHeader();

        return format;
    }
}
