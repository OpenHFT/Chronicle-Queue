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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.core.values.LongValue;

import net.openhft.chronicle.queue.Excerpt;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static net.openhft.chronicle.wire.Wires.isData;

/**
 * SingleChronicle implements Chronicle over a single streaming file <p> Created by peter.lawrey on 30/01/15.
 */
public class IndexedSingleChronicleQueue extends AbstractChronicle {

    static final long HEADER_OFFSET = 8L;
    static final long UNINITIALISED = 0L;
    static final long BUILDING = toLong("BUILDING");
    static final long QUEUE_CREATED = toLong("QUEUE400");
    static final int NOT_READY = Wires.NOT_READY;
    static final int META_DATA = Wires.META_DATA;
    static final int LENGTH_MASK = Wires.LENGTH_MASK;
    static final int MAX_LENGTH = LENGTH_MASK;
    // don't write to this without reviewing net.openhft.chronicle.queue.impl.SingleChronicleQueue.casMagicOffset
    private static final long MAGIC_OFFSET = 0L;
    private static final Logger LOG = LoggerFactory.getLogger(IndexedSingleChronicleQueue.class.getName());
    final Header header = new Header();
    @NotNull
    final Wire wire;
    private final ThreadLocal<ExcerptAppender> localAppender = new ThreadLocal<>();
    @NotNull
    private final MappedFile mappedFile;
    private final Bytes headerMemory;
    @NotNull
    private final Bytes bytes;

    private final Function<Bytes, Wire> bytesToWireFunction;
    // used in the indexer
    private final ThreadLocal<ByteableLongArrayValues> longArray;
    private final WireType wireType;
    private long firstBytes = -1;


    @ForceInline
    private static long toLong(String str) {
        final Bytes bytes = Bytes.allocateDirect(str.getBytes());
        return bytes.readLong();
    }


    public IndexedSingleChronicleQueue(@NotNull final String filename,
                                       long blockSize,
                                       @NotNull final WireType wireType) throws IOException {

        header.init(Compression.NONE, wireType);
        mappedFile = MappedFile.mappedFile(filename, blockSize);
        headerMemory = mappedFile.acquireBytesForWrite(0);
        bytes = mappedFile.acquireBytesForWrite(0);
        wire = createWire(wireType, bytes);
        this.wireType = wireType;
        bytesToWireFunction = wireType;
        longArray = Indexer.newLongArrayValuesPool(this.wireType);

        initialiseHeader();
    }

    private static Wire createWire(@NotNull final WireType wireType,
                                   @NotNull final Bytes bytes) {
        return wireType.apply(bytes);
    }

    static Function<Bytes, Wire> byteToWireFor(Class<? extends Wire> wireType) {
        if (TextWire.class.isAssignableFrom(wireType))
            return TextWire::new;
        else if (BinaryWire.class.isAssignableFrom(wireType))
            return BinaryWire::new;
        else if (RawWire.class.isAssignableFrom(wireType))
            return RawWire::new;
        else
            throw new UnsupportedOperationException("todo");
    }

    private void initialiseHeader() throws IOException {
        if (bytes.compareAndSwapLong(MAGIC_OFFSET, UNINITIALISED, BUILDING)) {
            buildHeader();
        }
        readHeader();
    }

    private void buildHeader() {
        // skip the magic number.
        bytes.writePosition(HEADER_OFFSET);

        wire.writeDocument(true, w -> w
                .write(MetaDataKey.header).marshallable(header.init(Compression.NONE, wireType)));

        if (!bytes.compareAndSwapLong(MAGIC_OFFSET, BUILDING, QUEUE_CREATED))
            throw new AssertionError("Concurrent writing of the header");
    }

    private void readHeader() throws IOException {
        // skip the magic number.
        waitForTheHeaderToBeBuilt(bytes);

        bytes.readPosition(HEADER_OFFSET);

        if (!wire.readDocument(w -> w.read().marshallable(header), null))
            throw new AssertionError("No header!?");
        firstBytes = bytes.writePosition();
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

    @Override
    public String name() {
        return mappedFile.toString();
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new SingleTailer(this, bytesToWireFunction, wireType);
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        ExcerptAppender appender = localAppender.get();
        if (appender == null)
            localAppender.set(appender = new SingleAppender(this, bytesToWireFunction));
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


    public long lastWrittenIndex() {
        return header.lastIndex().getVolatileValue();
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


    @Override
    protected Wire wire() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Class<? extends Wire> wireType() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return gets the index2index, or creates it, if it does not exist.
     */
    public long indexToIndex() {
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
     * Creates a new Excerpt containing and index which will be 1L << 17L bytes long, This method is used for creating
     * both the primary and secondary indexes. Chronicle Queue uses a root primary index ( each entry in the primary
     * index points to a unique a secondary index. The secondary index only records the address of every 64th except,
     * the except are linearly scanned from there on.
     *
     * @return the address of the Excerpt containing the usable index, just after the header
     */
    public long newIndex() {

        final LongArrayValues array = longArray.get();
        final long size = array.sizeInBytes(Indexer.NUMBER_OF_ENTRIES_IN_EACH_INDEX);
        final Bytes buffer = NativeBytes.nativeBytes(size);
        buffer.zeroOut(0, size);

        final Wire wire = new BinaryWire(buffer);
        wire.write(() -> "index").int64array(Indexer.NUMBER_OF_ENTRIES_IN_EACH_INDEX);
        //    buffer.flip();
        return appendMetaDataReturnAddress(buffer);

    }

    /**
     * This method does not update the index, as indexs are not used for meta data
     *
     * @param buffer
     * @return the address of the appended data
     */
    private long appendMetaDataReturnAddress(@NotNull Bytes buffer) {
        long length = buffer.writeRemaining();
        if (length > MAX_LENGTH)
            throw new IllegalStateException("Length too large: " + length);

        LongValue writeByte = header.writeByte();
        long lastByte = writeByte.getVolatileValue();

        for (; ; ) {

            if (bytes.compareAndSwapInt(lastByte, 0, NOT_READY | (int) length)) {
                long lastByte2 = lastByte + 4 + buffer.writeRemaining();
                bytes.write(lastByte + 4, buffer);
                writeByte.setOrderedValue(lastByte2);
                bytes.writeOrderedInt(lastByte, (int) (META_DATA | length));
                return lastByte;
            }
            int length2 = length30(bytes.readVolatileInt());
            bytes.writeSkip(length2);
           /* try {
                Jvm.checkInterrupted();
            } catch (InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            }*/
        }
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    public long appendDocument(@NotNull Bytes buffer) {
        long length = buffer.readRemaining();
        if (length > MAX_LENGTH)
            throw new IllegalStateException("Length too large: " + length);

        LongValue writeByte = header.writeByte();


        for (; ; ) {

            long lastByte = writeByte.getVolatileValue();

            if (bytes.compareAndSwapInt(lastByte, 0, NOT_READY | (int) length)) {
                long lastByte2 = lastByte + 4 + buffer.writeRemaining();
                bytes.write(lastByte + 4, buffer);
                long lastIndex = header.lastIndex().addAtomicValue(1);
                writeByte.setOrderedValue(lastByte2);
                bytes.writeOrderedInt(lastByte, (int) length);
                return lastIndex;
            }
            int length2 = length30(bytes.readVolatileInt());
            bytes.writeSkip(length2);
           /* try {
                Jvm.checkInterrupted();
            } catch (InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            }*/
        }
    }

    public boolean readDocument(long offset, ReadMarshallable buffer) {
        throw new UnsupportedOperationException("todo");
    }

    //@Override
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
    public Bytes bytes() {
        return bytes;
    }


    public long lastIndex() {
        long value = header.lastIndex().getVolatileValue();
        if (value == -1)
            throw new IllegalStateException("No data has been written to chronicle.");
        return value;
    }


    public boolean index(long index, @NotNull VanillaBytes bytes) {
        if (index == -1) {
            bytes.bytesStore(headerMemory, HEADER_OFFSET, headerMemory.length() - HEADER_OFFSET);
            return true;
        }
        return false;
    }


    public long firstBytes() {
        return firstBytes;
    }

    private int length30(int i) {
        return i & LENGTH_MASK;
    }

    enum MetaDataKey implements WireKey {
        header, index2index, index
    }

}


