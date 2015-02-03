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
    // fields which can be serialized/deserialized in the normal way.
    UUID uuid;
    ZonedDateTime created;
    String user;
    String host;

    // support binding to off heap memory with thread safe operations.
    final LongValue writeByte = newDirectInstance(LongValue.class);

    public Header init() {
        uuid = UUID.randomUUID();
        created = ZonedDateTime.now();
        user = System.getProperty("user.name");
        host = SingleChronicle.getHostName();
        writeByte.setOrderedValue(0);
        return this;
    }

    enum Field implements WireKey {
        type,
        uuid, created, user, host,
        readLock, readIndex, readByte,
        writeLock, writeIndex, writeByte
    }

    @Override
    public void writeMarshallable(WireOut out) {
        out.write(Field.uuid).uuid(uuid)
                .write(Field.created).zonedDateTime(created)
                .write(Field.user).text(user)
                .write(Field.host).text(host)
                .write(Field.writeByte).int64(writeByte);
    }

    @Override
    public void readMarshallable(WireIn in) {
        in.read(Field.uuid).uuid(u -> uuid = u)
                .read(Field.created).zonedDateTime(c -> created = c)
                .read(Field.user).text(u -> user = u)
                .read(Field.host).text(h -> host = h)
                .read(Field.writeByte).int64(writeByte);
    }

    public long getWriteByte() {
        return writeByte.getVolatileValue();
    }

    public void setWriteByteLazy(long writeByte) {
        this.writeByte.setOrderedValue(writeByte);
    }
}
