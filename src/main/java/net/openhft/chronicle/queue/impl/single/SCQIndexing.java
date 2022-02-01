/*
 * Copyright 2016-2020 https://chronicle.software
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.CleaningThreadLocal;
import net.openhft.chronicle.core.threads.ThreadLocalHelper;
import net.openhft.chronicle.core.values.LongArrayValues;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.StreamCorruptedException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.wire.Wires.NOT_INITIALIZED;

class SCQIndexing extends AbstractCloseable implements Demarshallable, WriteMarshallable, Closeable {
    private static final boolean IGNORE_INDEXING_FAILURE = Jvm.getBoolean("queue.ignoreIndexingFailure");
    private static final boolean REPORT_LINEAR_SCAN = Jvm.getBoolean("chronicle.queue.report.linear.scan.latency");

    final LongValue nextEntryToBeIndexed;
    private final int indexCount;
    private final int indexCountBits;
    private final int indexSpacing;
    private final int indexSpacingBits;
    private final LongValue index2Index;
    private final Supplier<LongArrayValues> longArraySupplier;
    @NotNull
    private final ThreadLocal<WeakReference<LongArrayValuesHolder>> index2indexArray;
    @NotNull
    private final ThreadLocal<WeakReference<LongArrayValuesHolder>> indexArray;
    @NotNull
    private final WriteMarshallable index2IndexTemplate;
    @NotNull
    private final WriteMarshallable indexTemplate;
    /** Extracted as field to prevent lambda creation on every method reference pass. */
    private final Function<Supplier<LongArrayValues>, LongArrayValuesHolder> arrayValuesSupplierCall = this::newLogArrayValuesHolder;

    LongValue writePosition;
    Sequence sequence;
    // visible for testing
    int linearScanCount;
    Collection<Closeable> closeables = new ArrayList<>();

    /**
     * used by {@link Demarshallable}
     *
     * @param wire a wire
     */
    @UsedViaReflection
    private SCQIndexing(@NotNull WireIn wire) {
        this(wire.read(IndexingFields.indexCount).int32(),
                wire.read(IndexingFields.indexSpacing).int32(),
                wire.read(IndexingFields.index2Index).int64ForBinding(null),
                wire.read(IndexingFields.lastIndex).int64ForBinding(null),
                wire::newLongArrayReference);
    }

    SCQIndexing(@NotNull WireType wireType, int indexCount, int indexSpacing) {
        this(indexCount,
                indexSpacing,
                wireType.newLongReference().get(),
                wireType.newLongReference().get(),
                wireType.newLongArrayReference());
    }

    private SCQIndexing(int indexCount, int indexSpacing, LongValue index2Index, LongValue nextEntryToBeIndexed, Supplier<LongArrayValues> longArraySupplier) {
        this.indexCount = indexCount;
        this.indexCountBits = Maths.intLog2(indexCount);
        this.indexSpacing = indexSpacing;
        this.indexSpacingBits = Maths.intLog2(indexSpacing);
        this.index2Index = index2Index;
        this.nextEntryToBeIndexed = nextEntryToBeIndexed;
        this.longArraySupplier = longArraySupplier;
        this.index2indexArray = CleaningThreadLocal.withCleanup(wr -> Closeable.closeQuietly(wr.get()));
        this.indexArray = CleaningThreadLocal.withCleanup(wr -> Closeable.closeQuietly(wr.get()));
        this.index2IndexTemplate = w -> w.writeEventName("index2index").int64array(indexCount);
        this.indexTemplate = w -> w.writeEventName("index").int64array(indexCount);
        disableThreadSafetyCheck(true);
    }

    private LongArrayValuesHolder newLogArrayValuesHolder(Supplier<LongArrayValues> las) {
        LongArrayValues values = las.get();
        LongArrayValuesHolder longArrayValuesHolder = new LongArrayValuesHolder(values);
        closeables.add(values);
        return longArrayValuesHolder;
    }

    @NotNull
    private LongArrayValuesHolder getIndex2IndexArray() {
        return ThreadLocalHelper.getTL(index2indexArray, longArraySupplier, arrayValuesSupplierCall);
    }

    @NotNull
    private LongArrayValuesHolder getIndexArray() {
        return ThreadLocalHelper.getTL(indexArray, longArraySupplier, arrayValuesSupplierCall);
    }

    public long toAddress0(long index) {
        throwExceptionIfClosed();

        long siftedIndex = index >> (indexSpacingBits + indexCountBits);
        long mask = indexCount - 1L;
        // convert to an offset
        return mask & siftedIndex;
    }

    long toAddress1(long index) {
        long siftedIndex = index >> indexSpacingBits;
        long mask = indexCount - 1L;
        // convert to an offset
        return mask & siftedIndex;
    }

/*    @Override
    protected boolean performCloseInBackground() {
        return true;
    }*/

    @Override
    protected void performClose() {
        closeQuietly(index2Index, nextEntryToBeIndexed);
        closeQuietly(closeables);
        closeables.clear();
        // Eagerly clean up the contents of thread locals but only for this thread.
        // The contents of the thread local for other threads will be cleaned up in
        // MappedFile.performRelease
        closeTL(indexArray);
        closeTL(index2indexArray);
    }

    private void closeTL(ThreadLocal<WeakReference<LongArrayValuesHolder>> tl) {
        WeakReference<LongArrayValuesHolder> weakReference = tl.get();
        if (weakReference == null)
            return;
        LongArrayValuesHolder holder = weakReference.get();
        if (holder != null)
            closeQuietly(holder.values());
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(IndexingFields.indexCount).int64(indexCount)
                .write(IndexingFields.indexSpacing).int64(indexSpacing)
                .write(IndexingFields.index2Index).int64forBinding(0L, index2Index)
                .write(IndexingFields.lastIndex).int64forBinding(0L, nextEntryToBeIndexed);
    }

    @NotNull
    private LongArrayValues arrayForAddress(@NotNull Wire wire, long secondaryAddress) {
        LongArrayValuesHolder holder = getIndexArray();
        if (holder.address() == secondaryAddress)
            return holder.values();
        holder.address(secondaryAddress);
        wire.bytes().readPositionRemaining(secondaryAddress, 4); // to read the header.
        wire.readMetaDataHeader();
        return array(wire, holder.values(), false);
    }

    @NotNull
    private LongArrayValues array(@NotNull WireIn w, @NotNull LongArrayValues using, boolean index2index) {
        final StringBuilder sb = Wires.acquireStringBuilder();
        long readPos = w.bytes().readPosition();
        @NotNull final ValueIn valueIn = w.readEventName(sb);
        String name = index2index ? "index2index" : "index";
        if (!name.contentEquals(sb))
            throw new IllegalStateException("expecting index, was " + sb + ", bytes: " + w.bytes().readPosition(readPos).toHexString());

        valueIn.int64array(using, this, (o1, o2) -> {
        });
        return using;
    }

    /**
     * Creates a new Excerpt containing and index which will be 1L << 17L bytes long, This method is used for creating both the primary and secondary
     * indexes. Chronicle Queue uses a root primary index ( each entry in the primary index points to a unique a secondary index. The secondary index
     * only records the addressForRead of every 64th except, the except are linearly scanned from there on.  )
     *
     * @param wire the current wire
     * @return the addressForRead of the Excerpt containing the usable index, just after the header
     */
    long newIndex(@NotNull WireOut wire, boolean index2index) throws StreamCorruptedException {
        long writePosition = this.writePosition.getVolatileValue();
        Bytes<?> bytes = wire.bytes();
        bytes.writePosition(writePosition);

        long position = wire.enterHeader(indexCount * 8L + 128);

        WriteMarshallable writer = index2index ? index2IndexTemplate : indexTemplate;
        writer.writeMarshallable(wire);
        wire.updateHeader(position, true, 0);

        return position;
    }

    long newIndex(@NotNull Wire wire, @NotNull LongArrayValues index2Index, long index2) throws StreamCorruptedException {
        try {
            long pos = newIndex(wire, false);
            if (!index2Index.compareAndSet(index2, NOT_INITIALIZED, pos)) {
                throw new IllegalStateException("Index " + index2 + " in index2index was altered while we hold the write lock!");
            }
            index2Index.setMaxUsed(index2 + 1);
            return pos;
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Moves the position to the {@code index} <p> The indexes are stored in many excerpts, so the index2index tells chronicle where ( in other words
     * the addressForRead of where ) the root first level targetIndex is stored. The indexing works like a tree, but only 2 levels deep, the root of
     * the tree is at index2index ( this first level targetIndex is 1MB in size and there is only one of them, it only holds the addresses of the
     * second level indexes, there will be many second level indexes ( created on demand ), each is about 1MB in size  (this second level targetIndex
     * only stores the position of every 64th excerpt (depending on RollCycle)), so from every 64th excerpt a linear scan occurs.
     *
     * @param ec    the data structure we are navigating
     * @param index the index we wish to move to
     * @return the position of the {@code targetIndex} or -1 if the index can not be found
     */
    @NotNull
    ScanResult moveToIndex(@NotNull final ExcerptContext ec, final long index) {
        ScanResult value = moveToIndex0(ec, index);
        if (value == null)
            return moveToIndexFromTheStart(ec, index);
        return value;
    }

    @NotNull
    private ScanResult moveToIndexFromTheStart(@NotNull ExcerptContext ec, long index) {
        try {
            Wire wire = ec.wire();
            wire.bytes().readPositionUnlimited(0);
            if (wire.readDataHeader())
                return linearScan(wire, index, 0, wire.bytes().readPosition());
        } catch (EOFException fallback) {
            return ScanResult.END_OF_FILE;
        }
        return ScanResult.NOT_FOUND;
    }

    // visible for testing
    @Nullable
    ScanResult moveToIndex0(@NotNull final ExcerptContext ec, final long index) {
        if (index2Index.getVolatileValue() == NOT_INITIALIZED)
            return null;

        Wire wire = ec.wireForIndex();
        LongArrayValues index2index = getIndex2index(wire);
        long primaryOffset = toAddress0(index);

        long secondaryAddress = 0;
        long startIndex = index & -indexSpacing;
        while (primaryOffset >= 0) {
            secondaryAddress = index2index.getValueAt(primaryOffset);
            if (secondaryAddress != 0)
                break;
            startIndex -= indexCount * indexSpacing;
            primaryOffset--;
        }

        if (secondaryAddress <= 0) {
            return null;
        }
        @NotNull final LongArrayValues array1 = arrayForAddress(wire, secondaryAddress);
        long secondaryOffset = toAddress1(index);

        do {
            long fromAddress = array1.getValueAt(secondaryOffset);
            if (fromAddress == 0) {
                secondaryOffset--;
                startIndex -= indexSpacing;
                continue;
            }

            if (index == startIndex) {
                ec.wire().bytes().readPositionUnlimited(fromAddress);
                return ScanResult.FOUND;
            } else {
                return linearScan(ec.wire(), index, startIndex, fromAddress);
            }
        } while (secondaryOffset >= 0);

        return null; // no index,
    }

    /**
     * moves the context to the index of {@code toIndex} by doing a linear scans form a {@code fromKnownIndex} at  {@code knownAddress} <p> note meta
     * data is skipped and does not count to the indexes
     *
     * @param wire           if successful, moves the context to an addressForRead relating to the index {@code toIndex }
     * @param toIndex        the index that we wish to move the context to
     * @param fromKnownIndex a know index ( used as a starting point )
     * @param knownAddress   a know addressForRead ( used as a starting point )
     * @see SCQIndexing#moveToIndex
     */

    @NotNull
    private ScanResult linearScan(@NotNull final Wire wire,
                                  final long toIndex,
                                  final long fromKnownIndex,
                                  final long knownAddress) {
        long start = System.nanoTime();
        if (toIndex == fromKnownIndex)
            return ScanResult.FOUND;
        ScanResult scanResult = linearScan0(wire, toIndex, fromKnownIndex, knownAddress);
        if (REPORT_LINEAR_SCAN)
            checkLinearScanTime(toIndex, fromKnownIndex, start);
        return scanResult;
    }

    private void checkLinearScanTime(final long toIndex, final long fromKnownIndex, final long
            start) {
        if (!Jvm.isAssertEnabled())
            return;

        long end = System.nanoTime();
        if (end > start + 100_000) {
            printLinearScanTime(toIndex, fromKnownIndex, start, end, "linearScan by index");
        }
    }

    private boolean printLinearScanTime(long toIndex, long fromKnownIndex, long start, long end,
                                        String desc) {
        StackTrace st = null;
        if (Jvm.isDebugEnabled(getClass())) {
            int time = Jvm.isArm() ? 20_000_000 : 250_000;
            // ignore the time for the first message
            if (toIndex > 0 && end > start + time)
                st = new StackTrace("This is a profile stack trace, not an ERROR");
        }

        long tookUS = (end - start) / 1000;
        Jvm.perf().on(getClass(), "Took " + tookUS + " us to " + desc + " from " +
                        fromKnownIndex + " to " + toIndex + " = (0x" + Long.toHexString(toIndex)
                        + "-0x" + Long.toHexString(fromKnownIndex) + ")=" +
                        (toIndex - fromKnownIndex),
                st);
        return true;
    }

    @NotNull
    private ScanResult linearScan0(@NotNull final Wire wire,
                                   final long toIndex,
                                   long fromKnownIndex,
                                   long knownAddress) {
        this.linearScanCount++;
        @NotNull final Bytes<?> bytes = wire.bytes();

        // optimized if the `toIndex` is the last sequence
        long lastAddress = writePosition.getVolatileValue();
        long lastIndex = this.sequence.getSequence(lastAddress);
        if (toIndex == lastIndex) {
            assert (lastAddress >= knownAddress && lastIndex >= fromKnownIndex);
            knownAddress = lastAddress;
            fromKnownIndex = lastIndex;
        }

        bytes.readPositionUnlimited(knownAddress);

        for (long i = fromKnownIndex; ; i++) {
            try {
                if (wire.readDataHeader()) {
                    if (i == toIndex) {
                        return ScanResult.FOUND;
                    }
                    int header = bytes.readVolatileInt();
                    if (Wires.isNotComplete(header)) { // or isEndOfFile
                        return ScanResult.NOT_REACHED;
                    }
                    bytes.readSkip(Wires.lengthOf(header));
                    continue;
                }
            } catch (EOFException fallback) {
                // reached the end of the file.
                if (i == toIndex) {
                    return ScanResult.END_OF_FILE;
                }
            }
            return i == toIndex ? ScanResult.NOT_FOUND : ScanResult.NOT_REACHED;
        }
    }

    ScanResult linearScanTo(final long toIndex, final long knownIndex, final ExcerptContext ec, final long knownAddress) {
        return linearScan(ec.wire(), toIndex, knownIndex, knownAddress);
    }

    long linearScanByPosition(@NotNull final Wire wire,
                              final long toPosition,
                              final long indexOfNext,
                              final long startAddress,
                              boolean inclusive) throws EOFException {
        long start = System.nanoTime();
        long index = linearScanByPosition0(wire, toPosition, indexOfNext, startAddress, inclusive);
        long end = System.nanoTime();
        int time = Jvm.isArm() ? 1_000_000 : 100_000;
        if (end > start + time) {
            printLinearScanTime(toPosition, startAddress, start, end, "linearScan by position");
        }
        return index;
    }

    long linearScanByPosition0(@NotNull final Wire wire,
                               final long toPosition,
                               long indexOfNext,
                               long startAddress,
                               boolean inclusive) throws EOFException {
        assert toPosition >= 0;
        Bytes<?> bytes = wire.bytes();
        long i;
        // optimized if the `toPosition` is the writePosition
        long lastAddress = writePosition.getVolatileValue();
        long lastIndex = this.sequence.getSequence(lastAddress);

        if (lastAddress > 0 && toPosition == lastAddress
                && lastIndex != Sequence.NOT_FOUND && lastIndex != Sequence.NOT_FOUND_RETRY) {
            bytes.readPositionUnlimited(toPosition);
            i = lastIndex - 1;
        } else {
            bytes.readPositionUnlimited(startAddress);
            i = indexOfNext - 1;
        }

        while (bytes.readPosition() <= toPosition) {
            WireIn.HeaderType headerType = wire.readDataHeader(true);
            if (headerType == WireIn.HeaderType.EOF) {
                if (toPosition == Long.MAX_VALUE)
                    return i;
                throw new EOFException();
            }

            if (!inclusive && toPosition == bytes.readPosition())
                return i;

            switch (headerType) {
                case NONE:
                    if (toPosition == Long.MAX_VALUE) {
                        return i;
                    }

                    int header = bytes.readVolatileInt(bytes.readPosition());
                    throw new IllegalArgumentException(
                            "You can't know the index for an entry which hasn't been written. " +
                                    "start: " + startAddress +
                                    ", at: " + bytes.readPosition() +
                                    ", header: " + Integer.toHexString(header) +
                                    ", toPos: " + toPosition);
                case META_DATA:
                    break;
                case DATA:
                    ++i;
                    break;
            }

            if (bytes.readPosition() == toPosition)
                return i;

            int header = bytes.readVolatileInt();
            int len = Wires.lengthOf(header);
            assert Wires.isReady(header);
            bytes.readSkip(len);
        }

        throw new IllegalArgumentException("position not the start of a message, bytes" +
                ".readPosition()=" + bytes.readPosition() + ",toPosition=" + toPosition);
    }

    long nextEntryToBeIndexed() {
        return nextEntryToBeIndexed.getVolatileValue();
    }

    long sequenceForPosition(@NotNull ExcerptContext ec,
                             final long position,
                             boolean inclusive) throws StreamCorruptedException {
        long indexOfNext = 0;
        long lastKnownAddress = 0;
        @NotNull Wire wire = ec.wireForIndex();
        try {
            final LongArrayValues index2indexArr = getIndex2index(wire);

            int used2 = Maths.toUInt31(index2indexArr.getUsed());
            assert used2 > 0;
            Outer:
            for (int index2 = used2 - 1; index2 >= 0; index2--) {
                long secondaryAddress = getSecondaryAddress(wire, index2indexArr, index2);
                if (secondaryAddress == 0)
                    continue;

                LongArrayValues indexValues = arrayForAddress(wire, secondaryAddress);
                // TODO use a binary rather than linear search

                // check the first one to see if any in the index is appropriate.
                int used = Maths.toUInt31(indexValues.getUsed());
                assert used >= 0;
                if (used == 0)
                    continue;

                long posN = indexValues.getVolatileValueAt(0);
                assert posN >= 0;
                if (posN > position)
                    continue;

                for (int index1 = used - 1; index1 >= 0; index1--) {
                    long pos = indexValues.getVolatileValueAt(index1);
                    // TODO pos shouldn't be 0, but holes in the index appear..
                    if (pos == 0 || pos > position) {
                        continue;
                    }
                    lastKnownAddress = pos;
                    indexOfNext = ((long) index2 << (indexCountBits + indexSpacingBits)) + (index1 << indexSpacingBits);

                    if (lastKnownAddress == position)
                        return indexOfNext;

                    break Outer;
                }
            }
        } catch (IllegalStateException e) {
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), "Attempt to find " + Long.toHexString(position), e);
        }
        try {
            return linearScanByPosition(wire, position, indexOfNext, lastKnownAddress, inclusive);
        } catch (EOFException e) {
            throw new IllegalStateException(e);
        }
    }

    void initIndex(@NotNull Wire wire) throws StreamCorruptedException {
        long index2Index = this.index2Index.getVolatileValue();

        if (index2Index != NOT_INITIALIZED)
            throw new IllegalStateException("Who wrote the index2index?");

        long index = newIndex(wire, true);
        this.index2Index.compareAndSwapValue(NOT_INITIALIZED, index);

        LongArrayValues index2index = getIndex2index(wire);
        newIndex(wire, index2index, 0);
    }

    private LongArrayValues getIndex2index(@NotNull Wire wire) {

        LongArrayValuesHolder holder = getIndex2IndexArray();
        LongArrayValues values = holder.values();
        if (((Byteable) values).bytesStore() != null)
            return values;
        final long indexToIndex = index2Index.getVolatileValue();

        try (DocumentContext ignored = wire.readingDocument(indexToIndex)) {
            return array(wire, values, true);
        }
    }

    // May throw UnrecoverableTimeoutException
    private long getSecondaryAddress(@NotNull Wire wire, @NotNull LongArrayValues index2indexArr, int index2) throws  StreamCorruptedException {
        long secondaryAddress = index2indexArr.getVolatileValueAt(index2);
        if (secondaryAddress == 0) {
            secondaryAddress = newIndex(wire, index2indexArr, index2);
            long sa = index2indexArr.getValueAt(index2);
            if (sa != secondaryAddress)
                throw new AssertionError();
        }

        return secondaryAddress;
    }

    /**
     * add an entry to the sequenceNumber, so stores the position of an sequenceNumber
     *
     * @param ec             the wire that used to store the data
     * @param sequenceNumber the sequenceNumber that the data will be stored to
     * @param position       the position the data is at
     * @throws UnrecoverableTimeoutException todo
     * @throws StreamCorruptedException      todo
     */
    void setPositionForSequenceNumber(@NotNull ExcerptContext ec,
                                      long sequenceNumber,
                                      long position) throws StreamCorruptedException {

        // only say for example index every 0,15,31st entry
        if (!indexable(sequenceNumber)) {
            return;
        }

        Wire wire = ec.wireForIndex();
        Bytes<?> bytes = wire.bytes();
        if (position > bytes.capacity())
            throw new IllegalArgumentException("pos: " + position);

        // find the index2index
        final LongArrayValues index2indexArr = getIndex2index(wire);
        if (((Byteable) index2indexArr).bytesStore() == null) {
            assert false;
            return;
        }

        int index2 = (int) ((sequenceNumber) >>> (indexCountBits + indexSpacingBits));
        if (index2 >= indexCount) {
            if (IGNORE_INDEXING_FAILURE) {
                return;
            }
            throw new IllegalStateException("Unable to index " + sequenceNumber + ", the number of entries exceeds max number for the current rollcycle");
        }
        long secondaryAddress = getSecondaryAddress(wire, index2indexArr, index2);
        if (secondaryAddress > bytes.capacity())
            throw new IllegalStateException("sa2: " + secondaryAddress);
        bytes.readLimitToCapacity();
        LongArrayValues indexValues = arrayForAddress(wire, secondaryAddress);
        int index3 = (int) ((sequenceNumber >>> indexSpacingBits) & (indexCount - 1));

        // check the last one first.
        long posN = indexValues.getValueAt(index3);
        if (posN == 0) {
            indexValues.setValueAt(index3, position);
            indexValues.setMaxUsed(index3 + 1L);
        } else {
            assert posN == position;
        }
        nextEntryToBeIndexed.setMaxValue(sequenceNumber + indexSpacing);
    }

    public boolean indexable(long index) {
        throwExceptionIfClosed();

        return (index & (indexSpacing - 1)) == 0;
    }

    public long lastSequenceNumber(@NotNull ExcerptContext ec, boolean approximate)
            throws StreamCorruptedException {
        throwExceptionIfClosed();

        Sequence sequence1 = this.sequence;
        if (approximate && sequence1 != null) {
            for (int i = 0; i < 128; i++) {

                long address = writePosition.getVolatileValue(0);
                if (address == 0)
                    return -1;
                long sequence = sequence1.getSequence(address);
                if (sequence == Sequence.NOT_FOUND_RETRY)
                    continue;
                if (sequence == Sequence.NOT_FOUND)
                    break;
                return sequence;
            }
        }

        return sequenceForPosition(ec, Long.MAX_VALUE, false);
    }

    int indexCount() {
        return indexCount;
    }

    int indexSpacing() {
        return indexSpacing;
    }

    long moveToEnd(final Wire wire) {
        Sequence sequence1 = this.sequence;
        if (sequence1 != null) {
            for (int i = 0; i < 128; i++) {

                long endAddress = writePosition.getVolatileValue(0);
                if (endAddress == 0)
                    return -1;
                long sequence = sequence1.getSequence(endAddress);
                if (sequence == Sequence.NOT_FOUND_RETRY)
                    continue;
                if (sequence == Sequence.NOT_FOUND)
                    return -1;

                Bytes<?> bytes = wire.bytes();
                if (wire.usePadding())
                    endAddress += BytesUtil.padOffset(endAddress);

                bytes.readPosition(endAddress);

                for (; ; ) {
                    int header = bytes.readVolatileInt(endAddress);
                    if (header == 0 || Wires.isNotComplete(header))
                        return sequence;

                    int len = Wires.lengthOf(header) + 4;
                    len += BytesUtil.padOffset(len);

                    bytes.readSkip(len );
                    endAddress += len;

                    if (Wires.isData(header))
                        sequence += 1;

                }
            }
        }
        return -1;
    }

    enum IndexingFields implements WireKey {
        indexCount, indexSpacing, index2Index,
        lastIndex // NOTE: the nextEntryToBeIndexed
    }

    static class LongArrayValuesHolder {
        private final LongArrayValues values;
        private long address;

        LongArrayValuesHolder(LongArrayValues values) {
            this.values = values;
            address = Long.MIN_VALUE;
        }

        public long address() {
            return address;
        }

        public void address(long address) {
            this.address = address;
        }

        public LongArrayValues values() {
            return values;
        }
    }
}
