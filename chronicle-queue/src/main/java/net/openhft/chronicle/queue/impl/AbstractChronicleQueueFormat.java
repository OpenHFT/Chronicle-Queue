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
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WireUtil;
import org.jetbrains.annotations.NotNull;

import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.function.Function;

public abstract class AbstractChronicleQueueFormat implements ChronicleQueueFormat {

    protected final Function<Bytes, Wire> wireSupplier;
    protected final Header header;

    protected AbstractChronicleQueueFormat(WireType wireType) {
        this.wireSupplier = WireUtil.wireSupplierFor(wireType);
        this.header = new Header();
    }

    @Override
    public void readMarshallable(WireIn wireIn) throws IllegalStateException {
        wireIn.readDocument(
            w -> w.read().marshallable(header),
            null
        );
    }

    @Override
    public void writeMarshallable(WireOut wireOut) {
        wireOut.writeDocument(
            true,
            w -> w.write(() -> "header").typedMarshallable(this.header)
        );
    }

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

        Header() {
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

        @NotNull
        public Header init() {
            uuid = UUID.randomUUID();
            created = ZonedDateTime.now();
            user = System.getProperty("user.name");
            host = "";
            return this;
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


            out.addPadding((int) (PADDED_SIZE - out.bytes().writePosition()));
        }

        @Override
        public void readMarshallable(@NotNull WireIn in) {
            in.read(Field.uuid).uuid(this, (o, i) -> o.uuid = i)
                .read(Field.writeByte).int64(this.writeByte)
                .read(Field.created).zonedDateTime(this, (o, i) -> o.created = i)
                .read(Field.user).text(this, (o, i) -> o.user = i)
                .read(Field.host).text(this, (o, i) -> o.host = i)
                .read(Field.indexCount).int32(this, (o, i) -> o.indexCount = i)
                .read(Field.indexSpacing).int32(this, (o, i) -> o.indexSpacing =i)
                .read(Field.index2Index).int64(this.index2Index)
                .read(Field.lastIndex).int64(this.lastIndex);
        }

        public long getWriteByte() {
            return writeByte().getVolatileValue();
        }

        public void setWriteByteLazy(long writeByte) {
            this.writeByte().setOrderedValue(writeByte);
        }
    }
}
