package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static net.openhft.chronicle.wire.Wires.isData;

/**
 * SingleChronicle implements Chronicle over a single streaming file <p> Created by peter.lawrey on
 * 30/01/15.
 */
public class SingleChronicleQueue implements ChronicleQueue, DirectChronicleQueue {

    // don't write to this without reviewing net.openhft.chronicle.queue.impl.SingleChronicleQueue.casMagicOffset
    private static final long MAGIC_OFFSET = 0L;

    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueue.class.getName());

    static final long HEADER_OFFSET = 8L;
    static final long UNINITIALISED = 0L;
    static final long BUILDING = asLong("BUILDING");
    static final long QUEUE_CREATED = asLong("QUEUE400");
    static final int NOT_READY = 1 << 30;
    static final int META_DATA = 1 << 31;
    static final int LENGTH_MASK = -1 >>> 2;
    static final int MAX_LENGTH = LENGTH_MASK;

    private final ThreadLocal<ExcerptAppender> localAppender = new ThreadLocal<>();
    @NotNull
    private final MappedFile mappedFile;
    private final Bytes headerMemory;
    private final Header header = new Header();
    @NotNull
    private final ChronicleWire wire;
    @NotNull
    private final Bytes bytes;
    private long firstBytes = -1;

    public SingleChronicleQueue(String filename, long blockSize) throws IOException {
        mappedFile = MappedFile.mappedFile(filename, blockSize);
        headerMemory = mappedFile.acquireBytes(0);
        bytes = mappedFile.bytes();
        wire = new ChronicleWire(new BinaryWire(bytes));
        initialiseHeader();
    }


    @Override
    public boolean readDocument(@NotNull AtomicLong offset, @NotNull Bytes buffer) {
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
            if (Thread.currentThread().isInterrupted())
                return false;
        }
    }

    @NotNull
    @Override
    public Bytes bytes() {
        return bytes;
    }

    @Override
    public long lastIndex() {
        long value = header.lastIndex().getVolatileValue();
        if (value == -1)
            throw new IllegalStateException("No data has been written to   chronicle.");
        return value;
    }

    @Override
    public boolean index(long index, @NotNull BytesStoreBytes bytes) {
        if (index == -1) {
            bytes.setByteStore(headerMemory, HEADER_OFFSET, headerMemory.length() - HEADER_OFFSET);
            return true;
        }
        return false;
    }

    private int length30(int i) {
        return i & LENGTH_MASK;
    }

    enum MetaDataKey implements WireKey {
        header, index2index, index
    }

    private void initialiseHeader() throws IOException {
        if (bytes.compareAndSwapLong(MAGIC_OFFSET, UNINITIALISED, BUILDING)) {
            buildHeader();
        }
        readHeader();
    }

    private void readHeader() throws IOException {
        // skip the magic number. 
        waitForTheHeaderToBeBuilt(bytes);

        bytes.position(HEADER_OFFSET);

        Consumer<WireIn> nullConsumer = o -> {
        };

        Consumer<WireIn> dataConsumer = $ -> {
            wire.read().marshallable(header);
        };

        wire.readDocument(dataConsumer, nullConsumer);
        firstBytes = bytes.position();
    }

    private void waitForTheHeaderToBeBuilt(@NotNull Bytes bytes) throws IOException {
        for (int i = 0; i < 1000; i++) {
            long magic = bytes.readVolatileLong(MAGIC_OFFSET);
            if (magic == BUILDING) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new IOException("Interrupted waiting for the header to be built");
                }
            } else if (magic == QUEUE_CREATED) {
                return;
            } else {
                throw new AssertionError("Invalid magic number " + Long.toHexString(magic) + " in file " + name());
            }
        }
        throw new AssertionError("Timeout waiting to build the file " + name());
    }

    private void buildHeader() {
        // skip the magic number.
        bytes.position(HEADER_OFFSET);

        wire.writeDocument(true, w -> w
                .write(MetaDataKey.header).marshallable(header.init(Compression.NONE)));

        if (!bytes.compareAndSwapLong(MAGIC_OFFSET, BUILDING, QUEUE_CREATED))
            throw new AssertionError("Concurrent writing of the header");
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

    private static long asLong(@NotNull String str) {
        ByteBuffer bb = ByteBuffer.wrap(str.getBytes(StandardCharsets.ISO_8859_1)).order(ByteOrder.nativeOrder());
        return bb.getLong();
    }

    @Override
    public String name() {
        return mappedFile.name();
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new SingleTailer(this);
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        ExcerptAppender appender = localAppender.get();
        if (appender == null)
            localAppender.set(appender = new SingleAppender(this));
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

    @Override
    public long firstBytes() {
        return firstBytes;
    }


    /**
     * @return gets the index2index, or creates it, if it does not exist.
     */
    long indexToIndex() {
        for (; ; ) {

            long index2Index = header.index2Index().getVolatileValue();

            if (index2Index == NOT_READY)
                continue;

            if (index2Index != UNINITIALISED)
                return index2Index;

            if (!header.index2Index().compareAndSwapValue(UNINITIALISED, NOT_READY))
                continue;

            long indexToIndex = newIndex();
            header.index2Index().setOrderedValue(indexToIndex);
            return indexToIndex;
        }
    }


    /**
     * Creates a new Excerpt containing and index which will be 1L << 17L bytes long, This method is
     * used for creating both the primary and secondary indexes. Chronicle Queue uses a root primary
     * index ( each entry in the primary index points to a unique a secondary index. The secondary
     * index only records the address of every 64th except, the except are linearly scanned from
     * there on.
     *
     * @return the address of the Excerpt containing the usable index, just after the header
     */
    long newIndex() {

        long indexSize = 1L << 17L;

        try (NativeStore<Void> allocate = NativeStore.nativeStore(6)) {

            final Bytes<Void> buffer = allocate.bytes();

            new BinaryWire(buffer).write(() -> "Index");
            buffer.flip();

            final long keyLen = buffer.limit();

            final long length = buffer.remaining();
            if (length > MAX_LENGTH)
                throw new IllegalStateException("Length too large: " + length);

            final LongValue writeByte = header.writeByte();
            final long lastByte = writeByte.getVolatileValue();

            for (; ; ) {
                if (bytes.compareAndSwapInt(lastByte, 0, NOT_READY | (int) length)) {
                    long lastByte2 = lastByte + 4 + buffer.remaining() + indexSize;
                    bytes.write(lastByte + 4, buffer);

                    header.lastIndex().addAtomicValue(1);
                    writeByte.setOrderedValue(lastByte2);
                    bytes.writeOrderedInt(lastByte, (int) (6 + indexSize));
                    long start = lastByte + 4;
                    bytes.zeroOut(start + keyLen, start + keyLen + length);
                    return start + keyLen;
                }
                int length2 = length30(bytes.readVolatileInt());
                bytes.skip(length2);
                Jvm.checkInterrupted();
            }
        } catch (Exception e) {
            throw new IORuntimeException(e);
        }


    }

    @Override
    public long appendDocument(@NotNull Bytes buffer) {
        long length = buffer.remaining();
        if (length > MAX_LENGTH)
            throw new IllegalStateException("Length too large: " + length);

        LongValue writeByte = header.writeByte();
        long lastByte = writeByte.getVolatileValue();

        for (; ; ) {

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

}

