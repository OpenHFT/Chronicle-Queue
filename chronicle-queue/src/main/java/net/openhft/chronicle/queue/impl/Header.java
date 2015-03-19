package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.Compression;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * Data structure to bind to an off heap representation.  This is required to support persistence
 * and sharing of this data structure.
 */
class Header implements Marshallable {
    private static final long PADDED_SIZE = 512;
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
    LongValue writeByte = null;
    LongValue index2Index = null;
    LongValue lastIndex = null;

    {
        lastIndex.setValue(-1);
    }

    @NotNull
    public Header init(@NotNull Compression compression) {
        uuid = UUID.randomUUID();
        created = ZonedDateTime.now();
        user = System.getProperty("user.name");
        host = SingleChronicleQueue.getHostName();
        this.compression = compression.name();
        writeByte.setOrderedValue(PADDED_SIZE);
        return this;
    }

    enum Field implements WireKey {
        type,
        uuid, created, user, host, compression,
        indexCount, indexSpacing,
        writeByte, index2Index
    }

    @Override
    public void writeMarshallable(@NotNull WireOut out) {
        out.write(Field.uuid).uuid(uuid)
                .write(Field.writeByte).int64(writeByte)
                .write(Field.created).zonedDateTime(created)
                .write(Field.user).text(user)
                .write(Field.host).text(host)
                .write(Field.compression).text(compression)
                .write(Field.indexCount).int32(indexCount)
                .write(Field.indexSpacing).int32(indexSpacing)
                .write(Field.index2Index).int64(index2Index);
        out.addPadding((int) (PADDED_SIZE - out.bytes().position()));
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
                .read(Field.index2Index).int64(index2Index, x -> index2Index = x);
    }

    public long getWriteByte() {
        return writeByte.getVolatileValue();
    }

    public void setWriteByteLazy(long writeByte) {
        this.writeByte.setOrderedValue(writeByte);
    }

    public static void main(String... args) {
        Header h = new Header();
        h.init(Compression.NONE);
        TextWire tw = new TextWire(NativeBytes.nativeBytes());
        tw.writeDocument(true, w -> w.write(() -> "header").marshallable(h));
        System.out.println(tw.bytes().flip().toString());

    }
}
