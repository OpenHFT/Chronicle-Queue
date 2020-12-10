/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://chronicle.software
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

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.Compression;
import org.jetbrains.annotations.NotNull;

import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * Data structure to bind to an off heap representation. This is required to
 * support persistence and sharing of this data structure.
 */
class Header implements Marshallable {
    public static final long PADDED_SIZE = 512;
    // fields which can be serialized/deserialized in the normal way.
    UUID uuid;
    ZonedDateTime created;
    String user;
    String host;
    String compression;
    int indexCount = 128 << 10;
    int indexSpacing = 64;

    // support binding to off heap memory with thread safe operations.

    // This is set to null as that it can pick up the right time the first time it is used.
    private LongValue writeByte = null;
    private LongValue index2Index = null;
    private LongValue lastIndex = null;

    public static void main(String... args) {
        Header h = new Header();
        h.init(Compression.NONE);
        TextWire tw = new TextWire(NativeBytes.nativeBytes());
        tw.writeDocument(true, w -> w.write("header").marshallable(h));
       // System.out.println(tw.bytes().flip().toString());
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
    public Header init(@NotNull Compression compression) {
        uuid = UUID.randomUUID();
        created = ZonedDateTime.now();
        user = System.getProperty("user.name");
        host = SingleChronicleQueue.getHostName();
        this.compression = compression.name();
        return this;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut out) {
        out.write(Field.uuid).uuid(uuid)
                .write(Field.writeByte).int64forBinding(PADDED_SIZE)
                .write(Field.created).zonedDateTime(created)
                .write(Field.user).text(user)
                .write(Field.host).text(host)
                .write(Field.compression).text(compression)
                .write(Field.indexCount).int32(indexCount)
                .write(Field.indexSpacing).int32(indexSpacing)
                .write(Field.index2Index).int64forBinding(0L)
                .write(Field.lastIndex).int64forBinding(-1L);

        // TODO: this is an hack and should be properly implemented.
        // The header is written as document which is enclosed between brackets
        // so it add a few more bytes thus the writeByte is invalid.
        if (out instanceof TextWire) {
            out.addPadding((int) (PADDED_SIZE - 2 - out.bytes().position()));

        } else {
            out.addPadding((int) (PADDED_SIZE - out.bytes().position()));
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn in) {
        in.read(Field.uuid).uuid(u -> uuid = u)
                .read(Field.writeByte).int64(writeByte, x -> writeByte = x)
                .read(Field.created).zonedDateTime(c -> created = c)
                .read(Field.user).text(u -> user = u)
                .read(Field.host).text(h -> host = h)
                .read(Field.compression).text(h -> compression = h)
                .read(Field.indexCount).int32(h -> indexCount = h)
                .read(Field.indexSpacing).int32(h -> indexSpacing = h)
                .read(Field.index2Index).int64(index2Index, x -> index2Index = x)
                .read(Field.lastIndex).int64(lastIndex, x -> lastIndex = x);
    }

    public long getWriteByte() {
        return writeByte().getVolatileValue();
    }

    public void setWriteByteLazy(long writeByte) {
        this.writeByte().setOrderedValue(writeByte);
    }

    enum Field implements WireKey {
        type,
        uuid, created, user, host, compression,
        indexCount, indexSpacing,
        writeByte, index2Index, lastIndex
    }
}
