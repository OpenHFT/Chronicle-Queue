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
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.AbstractChronicleQueueFormat;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.UUID;

import static net.openhft.chronicle.wire.WireUtil.wireCache;

class SingleChronicleQueueFormat extends AbstractChronicleQueueFormat {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Header.class, Header.CLASS_ALIAS);
    }

    private final SingleChronicleQueueBuilder builder;
    private final MappedFile mappedFile;
    private final BytesStore mappedStore;
    private final Header header;
    private final ThreadLocal<Wire> wireInCache;
    private final ThreadLocal<Wire> wireOutCache;

    SingleChronicleQueueFormat(final SingleChronicleQueueBuilder builder) throws IOException {
        super(builder.wireType());

        this.builder = builder;
        this.mappedFile = MappedFile.mappedFile(this.builder.path(), this.builder.blockSize());
        this.mappedStore = mappedFile.acquireByteStore(SPB_HEADER_BYTE);
        this.header = new Header();
        this.wireInCache = wireCache(mappedStore::bytesForRead, wireSupplier());
        this.wireOutCache = wireCache(mappedStore::bytesForWrite, wireSupplier());
    }

    // *************************************************************************
    //
    // *************************************************************************

    private SingleChronicleQueueFormat buildHeader() throws IOException {
        super.buildHeader(this.mappedStore, this.header);
        return this;
    }

    @Override
    public long append(@NotNull WriteMarshallable writer) throws IOException {
        for (long lastByte = header.getWriteByte(); ; ) {
            if(mappedStore.compareAndSwapInt(lastByte, WireUtil.FREE, WireUtil.BUILDING)) {
                final WireOut wo = wireOutCache.get();
                final Bytes wb = wo.bytes();

                wb.writePosition(lastByte);

                WireUtil.writeData(wo, writer);
                header.setWriteByteLazy(wb.writePosition());

                return header.incrementLastIndex();
            } else {
                int lastState = mappedStore.readInt(lastByte);
                if(WireUtil.isKnownLength(lastState)) {
                    lastByte += Wires.lengthOf(lastState) + SPB_DATA_HEADER_SIZE;
                } else {
                    // TODO: need to wait, waiting strategy ?
                }
            }
        }
    }

    /*
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

    protected boolean checkRemainingForAppend(@NotNull Bytes buffer) {
        long remaining = buffer.writeRemaining();
        if (remaining > WireUtil.LENGTH_MASK) {
            throw new IllegalStateException("Length too large: " + remaining);
        }

        return true;
    }
    */

    // *************************************************************************
    //
    // *************************************************************************

    public static SingleChronicleQueueFormat from(
            final SingleChronicleQueueBuilder builder) throws IOException {
        return new SingleChronicleQueueFormat(builder).buildHeader();
    }

    // *************************************************************************
    //
    // *************************************************************************

    private enum HeaderField implements WireKey {
        type,
        uuid, created, user, host,
        indexCount, indexSpacing,
        writeByte, index2Index, lastIndex
    }

    // TODO: is padded needed ?
    private class Header implements Marshallable {
        public static final String QUEUE_TYPE = "SCV4";
        public static final String CLASS_ALIAS = "Header";
        public static final long PADDED_SIZE = 512;

        // fields which can be serialized/deserialized in the normal way.
        private String type;
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

        Header() {
            this.type = QUEUE_TYPE;
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
            out.write(HeaderField.type).text(type)
                .write(HeaderField.uuid).uuid(uuid)
                .write(HeaderField.writeByte).int64forBinding(8)
                .write(HeaderField.created).zonedDateTime(created)
                .write(HeaderField.user).text(user)
                .write(HeaderField.host).text(host)
                .write(HeaderField.indexCount).int32(indexCount)
                .write(HeaderField.indexSpacing).int32(indexSpacing)
                .write(HeaderField.index2Index).int64forBinding(0L)
                .write(HeaderField.lastIndex).int64forBinding(-1L);
            //out.addPadding((int) (PADDED_SIZE - out.bytes().writePosition()));
        }

        @Override
        public void readMarshallable(@NotNull WireIn in) {
            in.read(HeaderField.type).text(this, (o, i) -> o.type = i)
                .read(HeaderField.uuid).uuid(this, (o, i) -> o.uuid = i)
                .read(HeaderField.writeByte).int64(this.writeByte, this, (o, i) -> o.writeByte = i)
                .read(HeaderField.created).zonedDateTime(this, (o, i) -> o.created = i)
                .read(HeaderField.user).text(this, (o, i) -> o.user = i)
                .read(HeaderField.host).text(this, (o, i) -> o.host = i)
                .read(HeaderField.indexCount).int32(this, (o, i) -> o.indexCount = i)
                .read(HeaderField.indexSpacing).int32(this, (o, i) -> o.indexSpacing = i)
                .read(HeaderField.index2Index).int64(this.index2Index, this, (o, i) -> o.index2Index = i)
                .read(HeaderField.lastIndex).int64(this.lastIndex, this, (o, i) -> o.lastIndex = i);
        }

        public long getWriteByte() {
            return writeByte.getVolatileValue();
        }

        public void setWriteByteLazy(long writeByte) {
            this.writeByte.setOrderedValue(writeByte);
        }

        public long incrementLastIndex() {
            return lastIndex.addAtomicValue(1);
        }
    }
}
