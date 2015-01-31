package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.lang.model.Out;
import net.openhft.lang.values.IntValue;
import net.openhft.lang.values.LongValue;

import java.io.StreamCorruptedException;
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
    final IntValue readLock = newDirectInstance(IntValue.class);
    final LongValue readIndex = newDirectInstance(LongValue.class);
    final LongValue readByte = newDirectInstance(LongValue.class);

    final IntValue writeLock = newDirectInstance(IntValue.class);
    final LongValue writeIndex = newDirectInstance(LongValue.class);
    final LongValue writeByte = newDirectInstance(LongValue.class);

    public void init() {
        uuid = UUID.randomUUID();
        created = ZonedDateTime.now();
        user = System.getProperty("user.name");
        host = SingleChronicle.getHostName();
        readLock.setOrderedValue(0);
        readIndex.setOrderedValue(0);
        readByte.setOrderedValue(0);
        writeLock.setOrderedValue(0);
        writeIndex.setOrderedValue(0);
        writeByte.setOrderedValue(0);
    }

    enum Field implements WireKey {
        type,
        uuid, created, user, host,
        readLock, readIndex, readByte,
        writeLock, writeIndex, writeByte
    }

    @Override
    public void writeMarshallable(WireOut out) {
        out.write(Field.type).text("header")
                .write(Field.uuid).uuid(uuid)
                .write(Field.created).zonedDateTime(created)
                .write(Field.user).text(user)
                .write(Field.host).text(host)
                .write(Field.readLock).int32(readLock)
                .write(Field.readIndex).int64(readIndex)
                .write(Field.readByte).int64(readByte)
                .write(Field.writeLock).cacheAlign().int32(writeLock)
                .write(Field.writeIndex).int64(writeIndex)
                .write(Field.writeByte).int64(writeByte);
    }

    @Override
    public void readMarshallable(WireIn in) throws StreamCorruptedException {
        in.read(Field.type).expectText("header")
                .read(Field.uuid).uuid(u -> uuid = u)
                .read(Field.created).zonedDateTime(c -> created = c)
                .read(Field.user).text(u -> user = u)
                .read(Field.host).text(h -> host = h)
                .read(Field.readLock).int32(readLock)
                .read(Field.readIndex).int64(readIndex)
                .read(Field.readByte).int64(readByte)
                .read(Field.writeLock).int32(writeLock)
                .read(Field.writeIndex).int64(writeIndex)
                .read(Field.writeByte).int64(writeByte);
    }

    public void getRead(@Out long[] offsets) throws InterruptedException {
        readLock.busyLockValue();
        try {
            offsets[0] = readByte.getValue();
            offsets[1] = readIndex.getValue();
        } finally {
            readLock.unlockValue();
        }
    }

    public void addToRead(long bytesForRead) throws InterruptedException {
        readLock.busyLockValue();
        try {
            readByte.addValue(bytesForRead);
            readIndex.addValue(1);
        } finally {
            readLock.unlockValue();
        }
    }

    public void addToWrite(@Out long[] offsets, long bytesToWrite) throws InterruptedException {
        writeLock.busyLockValue();
        try {
            offsets[0] = writeByte.addValue(bytesToWrite);
            offsets[1] = writeIndex.addValue(1);
        } finally {
            writeLock.unlockValue();
        }
    }
}
