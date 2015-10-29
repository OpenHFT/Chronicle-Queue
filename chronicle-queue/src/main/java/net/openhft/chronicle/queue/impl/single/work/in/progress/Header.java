/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single.work.in.progress;

import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * Data structure to bind to an off heap representation.  This is required to support persistence
 * and sharing of this data structure.
 */
public class Header implements Marshallable {
    public static final long PADDED_SIZE = 512;
    // fields which can be serialized/deserialized in the normal way.
    UUID uuid;
    ZonedDateTime created;
    String user;
    //String host;
    String compression;
    int indexCount = 128 << 10;
    int indexSpacing = 64;

    // support binding to off heap memory with thread safe operations.

    // This is set to null as that it can pick up the right time the first time it is used.
    private LongValue writeByte = null;
    private LongValue index2Index = null;
    private LongValue lastIndex = null;

    public LongValue writeByte() {
        return writeByte;
    }

    LongValue index2Index() {
        return index2Index;
    }

    LongValue lastIndex() {
        return lastIndex;
    }

    @NotNull
    public Header init(@NotNull Compression compression, WireType wireType) {
        uuid = UUID.randomUUID();
        created = ZonedDateTime.now();
        user = System.getProperty("user.name");
        //  host = IndexedSingleChronicleQueue.getHostName();
        this.compression = compression.name();

        writeByte = newLongValue(wireType);
        index2Index = newLongValue(wireType);
        lastIndex = newLongValue(wireType);

        return this;
    }

    LongValue newLongValue(WireType wireType) {
        LongValue value;
        if (wireType == WireType.BINARY) {
            BinaryLongReference value0 = new BinaryLongReference();
            value0.bytesStore(NativeBytes.nativeBytes(8), 0, 8);
            value = value0;
        } else if (wireType == WireType.TEXT) {
            TextLongReference value0 = new TextLongReference();
            value0.bytesStore(NativeBytes.nativeBytes(55), 0, 55);
            value = value0;
        } else {
            throw new IllegalStateException("type not supported");
        }
        return value;
    }


    enum Field implements WireKey {
        type,
        uuid, created, user, host, compression,
        indexCount, indexSpacing,
        writeByte, index2Index, lastIndex
    }

    @Override
    public void writeMarshallable(@NotNull net.openhft.chronicle.wire.WireOut out) {
        out.write(Field.uuid).uuid(uuid)
                .write(Field.writeByte).int64forBinding(PADDED_SIZE)
                .write(Field.created).zonedDateTime(created)
                .write(Field.user).text(user)
                // .writeBytes(Field.host).text(host)
                .write(Field.compression).text(compression)
                .write(Field.indexCount).int32(indexCount)
                .write(Field.indexSpacing).int32(indexSpacing)
                .write(Field.index2Index).int64forBinding(0L)
                .write(Field.lastIndex).int64forBinding(-1L);
        out.addPadding((int) (PADDED_SIZE - out.bytes().writePosition()));
    }

    @Override
    public void readMarshallable(@NotNull WireIn in) {
        in.read(Field.uuid).uuid(this, (o, u) -> o.uuid = u)
                .read(Field.writeByte).int64(this, (header, value) -> header.writeByte.setValue(value))
                .read(Field.created).zonedDateTime(this, (o, c) -> o.created = c)
                .read(Field.user).text(this, (o, u) -> o.user = u)
                //   .read(Field.host).text(this, (o, h) -> o.host = h)
                .read(Field.compression).text(this, (o, h) -> o.compression = h)
                .read(Field.indexCount).int32(this, (o, h) -> o.indexCount = h)
                .read(Field.indexSpacing).int32(this, (o, h) -> o.indexSpacing = h)
                .read(Field.index2Index).int64(this, (header, value) -> header.index2Index.setValue(value))
                .read(Field.lastIndex).int64(this, (header, value) -> header.lastIndex.setValue(value));
    }

    public long getWriteByte() {
        return writeByte().getVolatileValue();
    }


    public static void main(String... args) {
        Header h = new Header();
        h.init(Compression.NONE, WireType.TEXT);
        TextWire tw = new TextWire(NativeBytes.nativeBytes());
        tw.writeDocument(true, w -> w.write(() -> "header").marshallable(h));
        System.out.println(tw.bytes().toString());

    }
}
