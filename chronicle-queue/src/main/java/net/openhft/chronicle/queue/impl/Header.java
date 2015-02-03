package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.lang.values.LongValue;

import java.time.ZonedDateTime;
import java.util.UUID;

import static net.openhft.lang.model.DataValueClasses.newDirectInstance;

/**
 * Data structure to bind to an off heap representation.  This is required to support persistence and sharing of this
 * data structure.
 */
class Header implements Marshallable {
    private static final long PADDED_SIZE = 256;
    // fields which can be serialized/deserialized in the normal way.
    UUID uuid;
    ZonedDateTime created;
    String user;
    String host;

    // support binding to off heap memory with thread safe operations.
    final LongValue writeByte = newDirectInstance(LongValue.class);
    final LongValue index2Index = newDirectInstance(LongValue.class);

    public Header init() {
        uuid = UUID.randomUUID();
        created = ZonedDateTime.now();
        user = System.getProperty("user.name");
        host = SingleChronicle.getHostName();
        writeByte.setOrderedValue(PADDED_SIZE);
        return this;
    }

    enum Field implements WireKey {
        type,
        uuid, created, user, host,
        writeByte, index2Index
    }

    @Override
    public void writeMarshallable(WireOut out) {
        out.write(Field.uuid).uuid(uuid)
                .write(Field.writeByte).int64(writeByte)
                .write(Field.created).zonedDateTime(created)
                .write(Field.user).text(user)
                .write(Field.host).text(host)
                .write(Field.index2Index).int64(index2Index);
        out.addPadding((int) (256 - out.bytes().position()));
    }

    @Override
    public void readMarshallable(WireIn in) {
        in.read(Field.uuid).uuid(u -> uuid = u)
                .read(Field.writeByte).int64(writeByte)
                .read(Field.created).zonedDateTime(c -> created = c)
                .read(Field.user).text(u -> user = u)
                .read(Field.host).text(h -> host = h)
                .read(Field.index2Index).int64(index2Index);
    }

    public long getWriteByte() {
        return writeByte.getVolatileValue();
    }

    public void setWriteByteLazy(long writeByte) {
        this.writeByte.setOrderedValue(writeByte);
    }
}
