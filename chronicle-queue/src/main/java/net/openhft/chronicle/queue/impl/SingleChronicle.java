package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.Chronicle;
import net.openhft.chronicle.queue.Excerpt;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.*;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MappedFile;
import net.openhft.lang.io.MappedMemory;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.LongValue;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * SingleChronicle implements Chronicle over a single streaming file
 *
 * Created by peter on 30/01/15.
 */
public class SingleChronicle implements Chronicle {
    static final long MAGIC_OFFSET = 0L;
    static final long CREATED = 0L;
    static final long BUILDING = asLong("BUILDING");
    static final long QUEUE = asLong("QUEUE400");
    static final int META_BITS = 0x8000;
    static final int META_MASK = 0x7FFF;

    private final ThreadLocal<ExcerptAppender> localAppender = new ThreadLocal<>();
    private final MappedFile file;
    private final MappedMemory headerMemory;
    private final Header header = new Header();

    public SingleChronicle(String filename, long blockSize) throws IOException {
        file = new MappedFile(filename, blockSize);
        headerMemory = file.acquire(0);
        initialiseHeader();
    }

    private void initialiseHeader() throws IOException {
        Bytes bytes = headerMemory.bytes();
        long magic = bytes.readVolatileLong(MAGIC_OFFSET);
        if (magic == CREATED && bytes.compareAndSwapLong(MAGIC_OFFSET, CREATED, BUILDING)) {
            buildHeader();
        }
        readHeader();
    }

    private void readHeader() throws IOException {
        // skip the magic number. 
        Bytes bytes = headerMemory.bytes().bytes();
        waitForTheHeaderToBeBuilt(bytes);

        bytes.position(8L);

        int len = bytes.readUnsignedShort();
        if (len < META_BITS)
            throw new StreamCorruptedException("Length was " + Integer.toHexString(len));
        // shrink wrap the header so our padding can do it's work.
        bytes.limit(len + 8L);

        WireIn in = new BinaryWire(bytes, true, false, false);
        header.readMarshallable(in);

    }

    private void waitForTheHeaderToBeBuilt(Bytes bytes) throws IOException {
        for (int i = 0; ; i++) {
            long magic = bytes.readVolatileLong(MAGIC_OFFSET);
            if (magic != CREATED)
                throw new AssertionError("Invalid magic number " + Long.toHexString(magic) + " in file " + name());
            if (i > 1000)
                throw new AssertionError("Timeout waiting to build the file " + name());
            if (magic == BUILDING)
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new IOException("Interrupted waiting for the header to be built");
                }

        }
    }

    private void buildHeader() {
        Bytes bytes = headerMemory.bytes();
        // skip the magic number. 
        bytes.position(8L);

        long position = bytes.position();
        bytes.writeUnsignedShort(0);

        WireOut out = new BinaryWire(bytes, true, false, false);
        header.writeMarshallable(out);
        out.addPadding(1024);

        bytes.writeUnsignedShort(position, META_BITS | toShort(bytes.position() - position));

        if (!bytes.compareAndSwapLong(MAGIC_OFFSET, BUILDING, QUEUE))
            throw new AssertionError("Concurrent writing of the header");
    }

    static class Header implements Marshallable {
        UUID uuid;
        ZonedDateTime created;
        String user;
        String host;
        LongValue readReady = DataValueClasses.newDirectInstance(LongValue.class);
        LongValue readByte = DataValueClasses.newDirectInstance(LongValue.class);
        LongValue writeReady = DataValueClasses.newDirectInstance(LongValue.class);
        LongValue writeByte = DataValueClasses.newDirectInstance(LongValue.class);

        public void init() {
            uuid = UUID.randomUUID();
            created = ZonedDateTime.now();
            user = System.getProperty("user.name");
            host = getHostName();
            readReady.setOrderedValue(0);
            writeReady.setOrderedValue(0);
        }

        enum HeaderField implements WireKey {
            type,
            uuid, created, user, host,
            readReady, readByte,
            writeReady, writeByte
        }

        @Override
        public void writeMarshallable(WireOut out) {
            out.write(HeaderField.type).text("header")
                    .write(HeaderField.uuid).uuid(uuid)
                    .write(HeaderField.created).zonedDateTime(created)
                    .write(HeaderField.user).text(user)
                    .write(HeaderField.host).text(host)
                    .write(HeaderField.readReady).int64(readReady)
                    .write(HeaderField.readByte).int64(readByte)
                    .write(HeaderField.writeReady).cacheAlign().int64(writeReady)
                    .write(HeaderField.writeReady).int64(writeReady);
        }

        @Override
        public void readMarshallable(WireIn in) throws StreamCorruptedException {
            in.read(HeaderField.type).expectText("header")
                    .read(HeaderField.uuid).uuid(u -> uuid = u)
                    .read(HeaderField.created).zonedDateTime(c -> created = c)
                    .read(HeaderField.user).text(u -> user = u)
                    .read(HeaderField.host).text(h -> host = h)
                    .read(HeaderField.readReady).int64(readReady)
                    .read(HeaderField.readByte).int64(readByte)
                    .read(HeaderField.writeReady).int64(writeReady)
                    .read(HeaderField.writeByte).int64(writeByte);
        }
    }

    private static short toShort(long l) {
        if (l < 0 || l > Short.MAX_VALUE) throw new AssertionError();
        return (short) l;
    }

    static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            try {
                return Files.readAllLines(Paths.get("etc", "hostname")).get(0);
            } catch (Exception e2) {
                return "localhost";
            }
        }
    }

    private static long asLong(String str) {
        ByteBuffer bb = ByteBuffer.wrap(str.getBytes(StandardCharsets.ISO_8859_1)).order(ByteOrder.nativeOrder());
        return bb.getLong();
    }

    @Override
    public String name() {
        return file.name();
    }

    @Override
    public Excerpt createExcerpt() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new SingleTailer();
    }

    @Override
    public ExcerptAppender createAppender() throws IOException {
        ExcerptAppender appender = localAppender.get();
        if (appender == null)
            localAppender.set(appender = new SingleAppender());
        return appender;
    }

    @Override
    public long size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long firstAvailableIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lastWrittenIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException();
    }

}
