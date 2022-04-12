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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.wire.ByteableLongArrayValues;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.queue.impl.Indexer.NUMBER_OF_ENTRIES_IN_EACH_INDEX;
import static net.openhft.chronicle.wire.Wires.isData;

/**
 * SingleChronicle implements Chronicle over a single streaming file.
 * <p>
 * Created by peter.lawrey on 30/01/15.
 */
public class SingleChronicleQueue extends AbstractChronicle {

    static final long HEADER_OFFSET = 8L;
    static final long UNINITIALISED = 0L;
    static final long BUILDING = BytesUtil.asLong("BUILDING");
    static final long QUEUE_CREATED = BytesUtil.asLong("QUEUE400");
    static final int NOT_COMPLETE = Wires.NOT_COMPLETE;
    static final int META_DATA = Wires.META_DATA;
    static final int LENGTH_MASK = Wires.LENGTH_MASK;
    static final int MAX_LENGTH = LENGTH_MASK;

    private static final long MAGIC_OFFSET = 0L;

    @NotNull
    final Header header;

    @NotNull
    final Wire wire;

    @NotNull
    private final ThreadLocal<ExcerptAppender> localAppender;

    @NotNull
    private final MappedFile mappedFile;
    private final Bytes<?> headerMemory;

    @NotNull
    private final Bytes<?> bytes;
    //private final Function<Bytes<?>, Wire> bytesToWireFunction;

    // used in the indexer
    private final ThreadLocal<ByteableLongArrayValues> longArray;
    private final ChronicleQueueBuilder builder;
    private long firstBytes;

    public SingleChronicleQueue(@NotNull final ChronicleQueueBuilder builder) throws IOException {
        this.builder = builder.clone();
        this.header = new Header();
        this.localAppender = new ThreadLocal<>();
        this.firstBytes = -1;
        this.mappedFile = MappedFile.mappedFile(this.builder.name(), this.builder.blockSize());
        this.headerMemory = mappedFile.acquireBytes(0);
        this.bytes = mappedFile.bytes();
        this.wire = WireUtil.createWire(this.builder.wireType(), bytes);
        this.longArray = WireUtil.newLongArrayValuesPool(wireType());

        initialiseHeader();
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

    Wire createWire(@NotNull final Bytes<?> bytes) {
        return WireUtil.createWire(this.builder.wireType(), bytes);
    }

    private void initialiseHeader() throws IOException {
        if (bytes.compareAndSwapLong(MAGIC_OFFSET, UNINITIALISED, BUILDING)) {
            buildHeader();
        }
        readHeader();
    }

    private void buildHeader() {
        // skip the magic number.
        bytes.position(HEADER_OFFSET);

        wire.writeDocument(
                true,
                w -> w.write(MetaDataKey.header).marshallable(header.init(Compression.NONE))
        );

        if (!bytes.compareAndSwapLong(MAGIC_OFFSET, BUILDING, QUEUE_CREATED)) {
            throw new AssertionError("Concurrent writing of the header");
        }
    }

    private void readHeader() throws IOException {
        // skip the magic number.
        waitForTheHeaderToBeBuilt(bytes);

        bytes.position(HEADER_OFFSET);

        if (!wire.readDocument(w -> w.read().marshallable(header), null))
            throw new AssertionError("No header!?");
        firstBytes = bytes.position();
    }

    private void waitForTheHeaderToBeBuilt(@NotNull Bytes<?> bytes) throws IOException {
        for (int i = 0; i < 1000; i++) {
            long magic = bytes.readVolatileLong(MAGIC_OFFSET);
            if (magic == BUILDING) {
                try {
                    Jvm.pause(10);
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
        if (appender == null) {
            localAppender.set(appender = new SingleAppender(this));
        }

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
        return header.lastIndex().getValue();
    }

    @Override
    Wire wire() {
        return wire;
    }

    @Override
    public Class<? extends Wire> wireType() {
        return builder.wireType();
    }

    /**
     * @return gets the index2index, or creates it, if it does not exist.
     */
    long indexToIndex() {
        for (; ; ) {
            long index2Index = header.index2Index().getVolatileValue();

            if (index2Index == NOT_COMPLETE)
                continue;

            if (index2Index != UNINITIALISED)
                return index2Index;

            if (!header.index2Index().compareAndSwapValue(UNINITIALISED, NOT_COMPLETE))
                continue;

            long indexToIndex = newIndex();
            header.index2Index().setOrderedValue(indexToIndex);
            return indexToIndex;
        }
    }

    /**
     * Creates a new Excerpt containing and index which will be 1L << 17L bytes long, This method is used for creating
     * both the primary and secondary indexes. Chronicle Queue uses a root primary index ( each entry in the primary
     * index points to a unique a secondary index. The secondary index only records the addressForRead of every 64th except,
     * the except are linearly scanned from there on.
     *
     * @return the addressForRead of the Excerpt containing the usable index, just after the header
     */
    long newIndex() {
        final ByteableLongArrayValues array = longArray.get();

        final long size = array.sizeInBytes(NUMBER_OF_ENTRIES_IN_EACH_INDEX);
        final Bytes<?> buffer = NativeBytes.nativeBytes(size);
        buffer.zeroOut(0, size);

        final Wire wire = WireUtil.createWire(this.builder.wireType(), buffer);
        wire.write("index").int64array(NUMBER_OF_ENTRIES_IN_EACH_INDEX);
        buffer.flip();

        return appendMetaDataReturnAddress(buffer);
    }

    /**
     * This method does not update the index, as indexes are not used for meta data
     *
     * @param buffer
     * @return the addressForRead of the appended data
     */
    private long appendMetaDataReturnAddress(@NotNull Bytes<?> buffer) {
        long length = checkRemainingForAppend(buffer);

        LongValue writeByte = header.writeByte();
        long lastByte = writeByte.getVolatileValue();

        for (; ; ) {
            if (bytes.compareAndSwapInt(lastByte, 0, NOT_COMPLETE | (int) length)) {
                long lastByte2 = lastByte + 4 + buffer.remaining();
                bytes.write(lastByte + 4, buffer);
                writeByte.setOrderedValue(lastByte2);
                bytes.writeOrderedInt(lastByte, (int) (META_DATA | length));
                return lastByte;
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

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long appendDocument(@NotNull Bytes<?> buffer) {
        long length = checkRemainingForAppend(buffer);

        LongValue writeByte = header.writeByte();

        for (; ; ) {
            long lastByte = writeByte.getVolatileValue();

            if (bytes.compareAndSwapInt(lastByte, 0, NOT_COMPLETE | (int) length)) {
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

    @Override
    public boolean readDocument(@NotNull AtomicLong offset, @NotNull Bytes<?> buffer) {
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

    @NotNull
    @Override
    public Bytes<?> bytes() {
        return bytes;
    }

    @Override
    public long lastIndex() {
        long value = header.lastIndex().getVolatileValue();
        if (value == -1) {
            throw new IllegalStateException("No data has been written to chronicle.");
        }

        return value;
    }

    @Override
    public boolean index(long index, @NotNull VanillaBytes bytes) {
        if (index == -1) {
            bytes.bytesStore(headerMemory, HEADER_OFFSET, headerMemory.length() - HEADER_OFFSET);
            return true;
        }
        return false;
    }

    @Override
    public long firstBytes() {
        return firstBytes;
    }

    private int length30(int i) {
        return i & LENGTH_MASK;
    }

    protected long checkRemainingForAppend(@NotNull Bytes<?> buffer) {
        long remaining = buffer.remaining();
        if (remaining > MAX_LENGTH) {
            throw new IllegalStateException("Length too large: " + remaining);
        }

        return remaining;
    }

    enum MetaDataKey implements WireKey {
        header, index2index, index
    }
}
