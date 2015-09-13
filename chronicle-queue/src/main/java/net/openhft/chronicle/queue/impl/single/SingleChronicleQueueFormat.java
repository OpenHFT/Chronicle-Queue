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
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueueFormat;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireUtil;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.UUID;

import static net.openhft.chronicle.wire.WireUtil.readMeta;
import static net.openhft.chronicle.wire.WireUtil.wireIn;
import static net.openhft.chronicle.wire.WireUtil.wireOut;
import static net.openhft.chronicle.wire.WireUtil.writeMeta;

class SingleChronicleQueueFormat extends AbstractChronicleQueueFormat {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Header.class, "Header");
    }

    private final SingleChronicleQueueBuilder builder;
    private final MappedFile mappedFile;
    private final Header header;

    SingleChronicleQueueFormat(final SingleChronicleQueueBuilder builder) throws IOException {
        super(builder.wireType());

        this.builder = builder;
        this.mappedFile = MappedFile.mappedFile(this.builder.path(), this.builder.blockSize());
        this.header = new Header();
    }

    // *************************************************************************
    //
    // *************************************************************************

    public SingleChronicleQueueFormat init() throws IOException {

        final Bytes rb = this.mappedFile.acquireBytesForRead(WireUtil.SPB_HEADER_BYTE);
        final Bytes wb = this.mappedFile.acquireBytesForWrite(WireUtil.SPB_HEADER_BYTE);

        if(wb.compareAndSwapLong(WireUtil.SPB_HEADER_BYTE, WireUtil.SPB_HEADER_USET, WireUtil.SPB_HEADER_BUILDING)) {
            wb.writePosition(WireUtil.SPB_HEADER_BYTE_SIZE);

            writeMeta(
                wireOut(wb, wireSupplier()),
                w -> w.write(MetaDataKey.header).typedMarshallable(this.header)
            );

            if (!wb.compareAndSwapLong(WireUtil.SPB_HEADER_BYTE, WireUtil.SPB_HEADER_BUILDING, WireUtil.SPB_HEADER_BUILT)) {
                throw new AssertionError("Concurrent writing of the header");
            }
        }


        waitForTheHeaderToBeBuilt(rb);

        rb.readPosition(WireUtil.SPB_HEADER_BYTE_SIZE);
        readMeta(
            wireIn(rb, wireSupplier()),
            w -> w.read().marshallable(header)
        );

        return this;
    }

    // TODO: configure timeout/speep
    private void waitForTheHeaderToBeBuilt(@NotNull Bytes bytes) throws IOException {
        for (int i = 0; i < 1000; i++) {
            long magic = bytes.readVolatileLong(WireUtil.SPB_HEADER_BYTE);
            if (magic == WireUtil.SPB_HEADER_BUILDING) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new IOException("Interrupted waiting for the header to be built");
                }
            } else if (magic == WireUtil.SPB_HEADER_BUILT) {
                return;
            } else {
                throw new AssertionError(
                    "Invalid magic number " + Long.toHexString(magic) + " in file " + this.builder.path());
            }
        }

        throw new AssertionError("Timeout waiting to build the file " + this.builder.path());
    }

    /*
    @Override
    public long append(@NotNull Bytes buffer) {
        long length = checkRemainingForAppend(buffer);

        LongValue writeByte = header.writeByte();

        for (; ; ) {
            long lastByte = writeByte.getVolatileValue();

            if (bytes.compareAndSwapInt(lastByte, 0, NOT_READY | (int) length)) {
                long lastByte2 = lastByte + 4 + buffer.remaining();
                bytes.write(lastByte + 4, buffer);
                long lastIndex = header.lastIndex().addAtomicValue(1);
                writeByte.setOrderedValue(lastByte2);
                bytes.writeOrderedInt(lastByte, (int) length);
                return lastIndex;
            }
            int length2 = length30(bytes.readVolatileInt());
            bytes.skip(length2);
            try {
                Jvm.checkInterrupted();
            } catch (InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            }
        }
    }

    @Override
    public boolean read(@NotNull AtomicLong offset, @NotNull Bytes buffer) {
        buffer.clear();
        long lastByte = offset.get();
        for (; ; ) {
            int length = bytes.readVolatileInt(lastByte);
            int length2 = length30(length);
            if (Wires.isReady(length)) {
                lastByte += 4;
                buffer.write(bytes, lastByte, length2);
                lastByte += length2;
                offset.set(lastByte);
                return isData(length);
            }

            if (Thread.currentThread().isInterrupted()) {
                return false;
            }
        }
    }



    protected long checkRemainingForAppend(@NotNull Bytes buffer) {
        long remaining = buffer.remaining();
        if (remaining > MAX_LENGTH) {
            throw new IllegalStateException("Length too large: " + remaining);
        }

        return remaining;
    }
    */

    // *************************************************************************
    //
    // *************************************************************************

    enum MetaDataKey implements WireKey {
        header, index2index, index
    }

    enum Field implements WireKey {
        type,
        uuid, created, user, host,
        indexCount, indexSpacing,
        writeByte, index2Index, lastIndex
    }

    private class Header implements Marshallable {
        public static final long PADDED_SIZE = 512;

        // fields which can be serialized/deserialized in the normal way.
        private UUID uuid;
        private ZonedDateTime created;
        private String user;
        private String host;
        private int indexCount;
        private int indexSpacing;

        // support binding to off heap memory with thread safe operations.
        private LongValue writeByte;
        private LongValue index2Index;
        private LongValue lastIndex;

        private Bytes bytes;

        Header() {
            this.uuid = UUID.randomUUID();
            this.created = ZonedDateTime.now();
            this.user = System.getProperty("user.name");
            this.host = WireUtil.hostName();

            this.indexCount = 128 << 10;
            this.indexSpacing = 64;

            // This is set to null as that it can pick up the right time the
            // first time it is used.
            this.writeByte = null;
            this.index2Index = null;
            this.lastIndex = null;
        }

        LongValue writeByte() {
            return writeByte;
        }

        LongValue index2Index() {
            return index2Index;
        }

        LongValue lastIndex() {
            return lastIndex;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut out) {
            out.write(Field.uuid).uuid(uuid)
                .write(Field.writeByte).int64forBinding(PADDED_SIZE)
                .write(Field.created).zonedDateTime(created)
                .write(Field.user).text(user)
                .write(Field.host).text(host)
                .write(Field.indexCount).int32(indexCount)
                .write(Field.indexSpacing).int32(indexSpacing)
                .write(Field.index2Index).int64forBinding(0L)
                .write(Field.lastIndex).int64forBinding(-1L);
            //out.addPadding((int) (PADDED_SIZE - out.bytes().writePosition()));
        }

        @Override
        public void readMarshallable(@NotNull WireIn in) {
            in.read(Field.uuid).uuid(this, (o, i) -> o.uuid = i)
                .read(Field.writeByte).int64(this.writeByte, this, (o, i) -> o.writeByte = i)
                .read(Field.created).zonedDateTime(this, (o, i) -> o.created = i)
                .read(Field.user).text(this, (o, i) -> o.user = i)
                .read(Field.host).text(this, (o, i) -> o.host = i)
                .read(Field.indexCount).int32(this, (o, i) -> o.indexCount = i)
                .read(Field.indexSpacing).int32(this, (o, i) -> o.indexSpacing = i)
                .read(Field.index2Index).int64(this.index2Index, this, (o, i) -> o.index2Index = i)
                .read(Field.lastIndex).int64(this.lastIndex, this, (o, i ) -> o.lastIndex = i);
        }

        public long getWriteByte() {
            return writeByte().getVolatileValue();
        }

        public void setWriteByteLazy(long writeByte) {
            this.writeByte().setOrderedValue(writeByte);
        }
    }
}
