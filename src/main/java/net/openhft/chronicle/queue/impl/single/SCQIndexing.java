/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
import net.openhft.chronicle.core.scoped.ScopedResource;
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
import java.io.UncheckedIOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.queue.RollCycle.MAX_INDEX_COUNT;
import static net.openhft.chronicle.wire.Wires.NOT_INITIALIZED;

/**
 * SCQIndexing is responsible for managing index structures within a {@link SingleChronicleQueue}.
 * It stores and tracks positions of entries in a chronicle queue, optimizing access and scans for entries.
 * This class also maintains thread-local storage for index arrays and is capable of managing
 * write positions for entries.
 */
@SuppressWarnings("deprecation")
class SCQIndexing extends AbstractCloseable implements Indexing, Demarshallable, WriteMarshallable, Closeable {
    private static final boolean IGNORE_INDEXING_FAILURE = Jvm.getBoolean("queue.ignoreIndexingFailure");
    private static final boolean REPORT_LINEAR_SCAN = Jvm.getBoolean("chronicle.queue.report.linear.scan.latency");
    private static final long LINEAR_SCAN_WARN_THRESHOLD_NS = Long.getLong("linear.scan.warn.ns", 100_000);

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
    /**
     * Extracted as field to prevent lambda creation on every method reference pass.
     */
    private final Function<Supplier<LongArrayValues>, LongArrayValuesHolder> arrayValuesSupplierCall = this::newLogArrayValuesHolder;

    LongValue writePosition;
    Sequence sequence;
    // visible for testing
    int linearScanCount;
    int linearScanByPositionCount;
    Collection<Closeable> closeables = new ArrayList<>();
    private long lastScannedIndex = -1;

    /**
     * Constructor used for demarshalling via {@link Demarshallable}.
     *
     * @param wire The input wire to read from.
     */
    @UsedViaReflection
    private SCQIndexing(@NotNull WireIn wire) {
        this(wire.read(IndexingFields.indexCount).int32(),
                wire.read(IndexingFields.indexSpacing).int32(),
                wire.read(IndexingFields.index2Index).int64ForBinding(null),
                wire.read(IndexingFields.lastIndex).int64ForBinding(null),
                wire::newLongArrayReference);
    }

    /**
     * Constructor to create an {@code SCQIndexing} instance using a specific wire type.
     *
     * @param wireType    The wire type used for creating the index structure.
     * @param indexCount  The count of indexes.
     * @param indexSpacing The spacing between indexes.
     */
    SCQIndexing(@NotNull WireType wireType, int indexCount, int indexSpacing) {
        this(indexCount,
                indexSpacing,
                wireType.newLongReference().get(),
                wireType.newLongReference().get(),
                wireType.newLongArrayReference());
    }

    /**
     * Main constructor to initialize indexing with required parameters.
     *
     * @param indexCount         The count of indexes to maintain.
     * @param indexSpacing       The spacing between indexes.
     * @param index2Index        Reference for storing index-to-index values.
     * @param nextEntryToBeIndexed Reference for tracking the next entry to be indexed.
     * @param longArraySupplier  Supplier for creating long array values.
     */
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
        singleThreadedCheckDisabled(true);
    }

    // Helper method to create a new LongArrayValuesHolder
    private LongArrayValuesHolder newLogArrayValuesHolder(Supplier<LongArrayValues> las) {
        LongArrayValues values = las.get();
        LongArrayValuesHolder longArrayValuesHolder = new LongArrayValuesHolder(values);
        closeables.add(values);
        return longArrayValuesHolder;
    }

    // Fetches the index-to-index array from thread-local storage.
    @NotNull
    private LongArrayValuesHolder getIndex2IndexArray() {
        return ThreadLocalHelper.getTL(index2indexArray, longArraySupplier, arrayValuesSupplierCall);
    }

    // Fetches the index array from thread-local storage.
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

    /**
     * Closes this indexing instance, releasing resources.
     */
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

    // Helper method to close a thread-local LongArrayValuesHolder.
    private void closeTL(ThreadLocal<WeakReference<LongArrayValuesHolder>> tl) {
        WeakReference<LongArrayValuesHolder> weakReference = tl.get();
        if (weakReference == null)
            return;
        LongArrayValuesHolder holder = weakReference.get();
        if (holder != null)
            closeQuietly(holder.values());
    }

    /**
     * Serializes the indexing fields of this class to the provided {@link WireOut} object.
     * The fields include the index count, index spacing, and binding the {@code index2Index}
     * and {@code nextEntryToBeIndexed} fields.
     *
     * @param wire The {@link WireOut} object to which the data is written.
     */
    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(IndexingFields.indexCount).int64(indexCount)
                .write(IndexingFields.indexSpacing).int64(indexSpacing)
                .write(IndexingFields.index2Index).int64forBinding(0L, index2Index)
                .write(IndexingFields.lastIndex).int64forBinding(0L, nextEntryToBeIndexed);
    }

    /**
     * Retrieves the {@link LongArrayValues} stored at the specified secondary address within the wire.
     * If the secondary address matches the previously used address, the cached array is returned.
     * Otherwise, the new array is read from the wire.
     *
     * @param wire The wire containing the array data.
     * @param secondaryAddress The address to fetch the array from.
     * @return The {@link LongArrayValues} at the specified address.
     */
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

    /**
     * Reads an array of {@link LongArrayValues} from the wire and fills the specified {@code using} array.
     *
     * @param w The wire to read the array from.
     * @param using The {@link LongArrayValues} instance to populate.
     * @param index2index Whether the array being read is the index2index array.
     * @return The populated {@link LongArrayValues} instance.
     */
    @NotNull
    private LongArrayValues array(@NotNull WireIn w, @NotNull LongArrayValues using, boolean index2index) {
        @NotNull final ValueIn valueIn = readIndexValue(w, index2index ? "index2index" : "index");
        valueIn.int64array(using, this, (o1, o2) -> {
        });
        return using;
    }

    /**
     * Reads a value from the wire and checks if the event name matches the expected name.
     * Throws an {@link IllegalStateException} if the names do not match.
     *
     * @param w The wire to read the value from.
     * @param expectedName The expected event name.
     * @return The {@link ValueIn} corresponding to the expected event.
     */
    private ValueIn readIndexValue(@NotNull WireIn w, @NotNull String expectedName) {
        try (ScopedResource<StringBuilder> stlSb = Wires.acquireStringBuilderScoped()) {
            final StringBuilder sb = stlSb.get();
            long readPos = w.bytes().readPosition();
            @NotNull final ValueIn valueIn = w.readEventName(sb);
            if (!expectedName.contentEquals(sb))
                throw new IllegalStateException("expecting " + expectedName + ", was " + sb + ", bytes: " + w.bytes().readPosition(readPos).toHexString());
            return valueIn;
        }
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

    /**
     * Performs a linear scan from the start of the wire to find the specified {@code index}.
     *
     * @param ec The excerpt context used for reading the index.
     * @param index The index to find.
     * @return A {@link ScanResult} indicating the result of the operation.
     */
    @NotNull
    private ScanResult moveToIndexFromTheStart(@NotNull ExcerptContext ec, long index) {
        try {
            Wire wire = ec.wire();
            if (wire == null)
                return ScanResult.END_OF_FILE;

            wire.bytes().readPositionUnlimited(0);
            if (wire.readDataHeader())
                return linearScan(wire, index, 0, wire.bytes().readPosition());
        } catch (EOFException fallback) {
            return ScanResult.END_OF_FILE;
        }
        return ScanResult.NOT_FOUND;
    }

    /**
     * Attempts to move the context to the specified {@code index}. This method navigates through the index structure
     * stored in the wire and retrieves the corresponding address. It begins by using the primary index (index2index)
     * to locate the secondary index, and then performs a backwards scan in the secondary index.
     *
     * @param ec    The {@link ExcerptContext} used for reading the index.
     * @param index The index to move to.
     * @return A {@link ScanResult} indicating the result of the operation, or {@code null} if the index cannot be found.
     */
    @Nullable
    ScanResult moveToIndex0(@NotNull final ExcerptContext ec, final long index) {
        if (index2Index.getVolatileValue() == NOT_INITIALIZED)
            return null;

        Wire wireForIndex = ec.wireForIndex();
        LongArrayValues index2index = getIndex2index(wireForIndex);
        long primaryOffset = toAddress0(index);

        long secondaryAddress = 0;
        long startIndex = index & -indexSpacing;
        while (primaryOffset >= 0) {
            secondaryAddress = index2index.getValueAt(primaryOffset);
            if (secondaryAddress != 0) {
                @NotNull final LongArrayValues array1 = arrayForAddress(wireForIndex, secondaryAddress);
                ScanResult result = scanSecondaryIndexBackwards(ec, array1, startIndex, index);
                if (result != null)
                    return result;
            }
            startIndex -= (long) indexCount * indexSpacing;
            primaryOffset--;
        }

        return null;
    }

    /**
     * Performs a backwards scan of the secondary index to locate the specified {@code index}. If the index is found,
     * the position is moved to the address corresponding to the index. Otherwise, it performs a linear scan from
     * the starting point.
     *
     * @param ec         The {@link ExcerptContext} used for reading the index.
     * @param array1     The secondary index to scan.
     * @param startIndex The starting index of the scan.
     * @param index      The target index to find.
     * @return A {@link ScanResult} indicating whether the index was found, or if a linear scan is required.
     */
    private ScanResult scanSecondaryIndexBackwards(@NotNull final ExcerptContext ec, LongArrayValues array1, long startIndex, long index) {
        long secondaryOffset = toAddress1(index);

        do {
            long fromAddress = array1.getValueAt(secondaryOffset);
            if (fromAddress == 0) {
                secondaryOffset--;
                startIndex -= indexSpacing;
                continue;
            }

            Wire wire = ec.wire();
            if (wire == null)
                break;
            if (index == startIndex) {
                wire.bytes().readPositionUnlimited(fromAddress);
                return ScanResult.FOUND;
            } else {
                return linearScan(wire, index, startIndex, fromAddress);
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
        if (toIndex == fromKnownIndex)
            return ScanResult.FOUND;
        long start = REPORT_LINEAR_SCAN ? System.nanoTime() : 0;
        ScanResult scanResult = linearScan0(wire, toIndex, fromKnownIndex, knownAddress);
        if (REPORT_LINEAR_SCAN) {
            printLinearScanTime(lastScannedIndex, fromKnownIndex, start, "linearScan by index");
        }
        return scanResult;
    }

    /**
     * Prints the time taken for a linear scan operation, if it exceeds a threshold.
     * The method also records a stack trace if debugging is enabled.
     *
     * @param toIndex        The target index.
     * @param fromKnownIndex The known starting index.
     * @param start          The start time of the scan.
     * @param desc           A description of the scan operation.
     */
    private void printLinearScanTime(long toIndex, long fromKnownIndex, long start, String desc) {
        // still warming up?
        if (toIndex <= 1)
            return;

        // took too long to scan?
        long end = System.nanoTime();
        if (end < start + LINEAR_SCAN_WARN_THRESHOLD_NS)
            return;

        doPrintLinearScanTime(toIndex, fromKnownIndex, start, desc, end);
    }

    /**
     * Logs the details of a linear scan operation that took longer than expected, along with a stack trace if debugging is enabled.
     *
     * @param toIndex        The target index.
     * @param fromKnownIndex The known starting index.
     * @param start          The start time of the scan.
     * @param desc           A description of the scan operation.
     * @param end            The end time of the scan.
     */
    private void doPrintLinearScanTime(long toIndex, long fromKnownIndex, long start, String desc, long end) {
        StackTrace st = null;
        if (Jvm.isDebugEnabled(getClass())) {
            int time = Jvm.isArm() ? 20_000_000 : 250_000;
            // ignore the time for the first message
            if (toIndex > 0 && end > start + time)
                st = new StackTrace("This is a profile stack trace, not an ERROR");
        }

        long tookUS = (end - start) / 1000;
        String message = "Took " + tookUS + " us to " + desc + " " +
                fromKnownIndex + " to index " + toIndex;
        Jvm.perf().on(getClass(), message, st);
    }

    /**
     * Performs the actual linear scan operation from a known index and address to the target {@code toIndex}.
     *
     * @param wire           The wire used to read the data.
     * @param toIndex        The target index to reach.
     * @param fromKnownIndex The known starting index.
     * @param knownAddress   The address corresponding to the known starting index.
     * @return A {@link ScanResult} indicating the outcome of the scan.
     */
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
                        lastScannedIndex = i;
                        return ScanResult.FOUND;
                    }
                    int header = bytes.readVolatileInt();
                    if (Wires.isNotComplete(header)) { // or isEndOfFile
                        lastScannedIndex = i;
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
            lastScannedIndex = i;
            return i == toIndex ? ScanResult.NOT_FOUND : ScanResult.NOT_REACHED;
        }
    }

    /**
     * Performs a linear scan from a known index and address to a target {@code toIndex}.
     * This method leverages the wire's data and metadata headers to navigate the records
     * within the Chronicle Queue.
     *
     * @param toIndex      The target index to scan to.
     * @param knownIndex   A known index to start the scan from.
     * @param ec           The {@link ExcerptContext} used for reading the index.
     * @param knownAddress The address corresponding to the known index.
     * @return A {@link ScanResult} indicating the outcome of the scan.
     */
    ScanResult linearScanTo(final long toIndex, final long knownIndex, final ExcerptContext ec, final long knownAddress) {
        return linearScan(ec.wire(), toIndex, knownIndex, knownAddress);
    }

    /**
     * Performs a linear scan by position to locate the entry at the specified {@code toPosition}.
     * This method returns the index of the entry or an exception if the position is not valid.
     *
     * @param wire        The wire object used to read the data.
     * @param toPosition  The target position in the wire.
     * @param indexOfNext The index of the next known entry.
     * @param startAddress The starting address to begin the scan from.
     * @param inclusive   Whether the target position should be inclusive.
     * @return The index of the found entry.
     * @throws EOFException If the scan reaches the end of the file before finding the position.
     */
    long linearScanByPosition(@NotNull final Wire wire,
                              final long toPosition,
                              final long indexOfNext,
                              final long startAddress,
                              boolean inclusive) throws EOFException {
        long start = REPORT_LINEAR_SCAN ? System.nanoTime() : 0;
        long index = linearScanByPosition0(wire, toPosition, indexOfNext, startAddress, inclusive);
        if (REPORT_LINEAR_SCAN) {
            printLinearScanTime(index, startAddress, start, "linearScan by position");
        }
        return index;
    }

    /**
     * Helper method to perform the actual linear scan by position. This method reads through
     * the wire's entries, navigating based on position and header type, until it finds the
     * required position or reaches the end of the wire.
     *
     * @param wire         The wire object used to read the data.
     * @param toPosition   The target position in the wire.
     * @param indexOfNext  The index of the next known entry.
     * @param startAddress The starting address to begin the scan from.
     * @param inclusive    Whether the target position should be inclusive.
     * @return The index of the found entry.
     * @throws EOFException If the scan reaches the end of the file before finding the position.
     */
    long linearScanByPosition0(@NotNull final Wire wire,
                               final long toPosition,
                               long indexOfNext,
                               long startAddress,
                               boolean inclusive) throws EOFException {
        linearScanByPositionCount++;
        assert toPosition >= 0;
        Bytes<?> bytes = wire.bytes();
        long i;

        // Optimized path if the `toPosition` is the last written position.
        long lastAddress = writePosition.getVolatileValue();
        long lastIndex = this.sequence.getSequence(lastAddress);

        i = calculateInitialValue(toPosition, indexOfNext, startAddress, bytes, lastAddress, lastIndex);

        // Scan through the entries until the target position is found or exceeded.
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
                    // Case where no data header is found
                    if (toPosition == Long.MAX_VALUE) {
                        return i;
                    }

                    int header = bytes.readVolatileInt(bytes.readPosition());
                    throwIndexNotWritten(toPosition, startAddress, bytes, header);
                    break;
                case META_DATA:
                    // Skip metadata
                    break;
                case DATA:
                    // Increment the index for each valid data entry
                    ++i;
                    break;
                case EOF:
                    throw new AssertionError("EOF should have been handled");
            }

            // If the current position matches the target, return the index
            if (bytes.readPosition() == toPosition)
                return i;

            // Skip over the current entry
            int header = bytes.readVolatileInt();
            int len = Wires.lengthOf(header);
            assert Wires.isReady(header);
            bytes.readSkip(len);
        }

        return throwPositionNotAtStartOfMessage(toPosition, bytes);
    }

    /**
     * Calculates the initial index value to start scanning from, based on whether
     * the target position is the last written position or a known earlier position.
     *
     * @param toPosition   The target position in the wire.
     * @param indexOfNext  The index of the next known entry.
     * @param startAddress The starting address for the scan.
     * @param bytes        The bytes object associated with the wire.
     * @param lastAddress  The address of the last written entry.
     * @param lastIndex    The index of the last written entry.
     * @return The starting index for the scan.
     */
    private long calculateInitialValue(long toPosition, long indexOfNext, long startAddress, Bytes<?> bytes, long lastAddress, long lastIndex) {
        if (lastAddress > 0 && toPosition == lastAddress
                && lastIndex != Sequence.NOT_FOUND && lastIndex != Sequence.NOT_FOUND_RETRY) {
            bytes.readPositionUnlimited(toPosition);
            return lastIndex - 1;
        } else {
            bytes.readPositionUnlimited(startAddress);
            return indexOfNext - 1;
        }
    }

    /**
     * Throws an exception if an index is requested for an entry that hasn't been written yet.
     *
     * @param toPosition   The target position in the wire.
     * @param startAddress The starting address for the scan.
     * @param bytes        The bytes object associated with the wire.
     * @param header       The header of the current entry.
     */
    private void throwIndexNotWritten(long toPosition, long startAddress, Bytes<?> bytes, int header) {
        throw new IllegalArgumentException(
                "You can't know the index for an entry which hasn't been written. " +
                        "start: " + startAddress +
                        ", at: " + bytes.readPosition() +
                        ", header: " + Integer.toHexString(header) +
                        ", toPos: " + toPosition);
    }

    /**
     * Throws an exception if the position is not at the start of a message, meaning the scan failed
     * to locate a valid message at the specified position.
     *
     * @param toPosition The target position in the wire.
     * @param bytes      The bytes object associated with the wire.
     * @return A long indicating the failure.
     */
    private long throwPositionNotAtStartOfMessage(long toPosition, Bytes<?> bytes) {
        throw new IllegalArgumentException("position not the start of a message, bytes" +
                ".readPosition()=" + bytes.readPosition() + ",toPosition=" + toPosition);
    }

    @Override
    public long nextEntryToBeIndexed() {
        return nextEntryToBeIndexed.getVolatileValue();
    }

    /**
     * Returns the sequence number for a given position in the wire.
     * If an exact match is found for the position, the corresponding index is returned;
     * otherwise, a linear scan is performed to approximate the closest sequence.
     *
     * @param ec        The {@link ExcerptContext} used to navigate the queue.
     * @param position  The position for which the sequence is requested.
     * @param inclusive Whether the position should be treated inclusively.
     * @return The sequence number for the given position, or an approximation based on the linear scan.
     * @throws StreamCorruptedException If the index is corrupted or not initialized properly.
     */
    long sequenceForPosition(@NotNull ExcerptContext ec,
                             final long position,
                             boolean inclusive) throws StreamCorruptedException {
        long indexOfNext = 0;
        long lastKnownAddress = 0;
        @NotNull Wire wire = ec.wireForIndex();
        try {
            final LongArrayValues index2indexArr = getIndex2index(wire);
            int used2 = getUsedAsInt(index2indexArr);

            // Outer loop: Iterate through index2index array to find the relevant secondary index.
            Outer:
            for (int index2 = used2 - 1; index2 >= 0; index2--) {
                long secondaryAddress = getSecondaryAddress(wire, index2indexArr, index2);
                if (secondaryAddress == 0)
                    continue;

                LongArrayValues indexValues = arrayForAddress(wire, secondaryAddress);
                // TODO use a binary rather than linear search

                // check the first one to see if any in the index is appropriate.
                int used = getUsedAsInt(indexValues);
                if (used == 0)
                    continue;

                // Check if the first value in the index is appropriate.
                long posN = indexValues.getVolatileValueAt(0);
                assert posN >= 0;
                if (posN > position)
                    continue;

                // Inner loop: Search within the secondary index.
                for (int index1 = used - 1; index1 >= 0; index1--) {
                    long pos = indexValues.getVolatileValueAt(index1);
                    // TODO pos shouldn't be 0, but holes in the index appear..
                    if (pos == 0 || pos > position) {
                        continue;
                    }
                    lastKnownAddress = pos;
                    indexOfNext = ((long) index2 << (indexCountBits + indexSpacingBits)) + ((long) index1 << indexSpacingBits);

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
            // Perform a linear scan if no exact match is found.
            return linearScanByPosition(wire, position, indexOfNext, lastKnownAddress, inclusive);
        } catch (EOFException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Retrieves the number of entries used in the given {@link LongArrayValues}.
     * Validates that the number of entries is within the expected range.
     *
     * @param index2indexArr The {@link LongArrayValues} representing the index.
     * @return The number of entries used, as an integer.
     */
    static int getUsedAsInt(LongArrayValues index2indexArr) {
        if (((Byteable) index2indexArr).bytesStore() == null)
            return 0;

        final long used = index2indexArr.getUsed();
        if (used < 0 || used > MAX_INDEX_COUNT)
            throw new IllegalStateException("Used: " + used);
        return (int) used;
    }

    /**
     * Initializes the index and sets up the index2index and other structures required for indexing.
     * This method sets the position and creates new entries in the index.
     *
     * @param wire The {@link Wire} object used to write to the queue.
     * @throws StreamCorruptedException If the index is corrupted or not initialized properly.
     */
    void initIndex(@NotNull Wire wire) throws StreamCorruptedException {
        long index2Index = this.index2Index.getVolatileValue();

        if (index2Index != NOT_INITIALIZED)
            throw new IllegalStateException("Who wrote the index2index?");

        // Ensure new header position is found despite the first header not being finalized.
        long oldPos = wire.bytes().writePosition();
        if (!writePosition.compareAndSwapValue(0, oldPos))
            throw new IllegalStateException("Who updated the position?");

        long index = newIndex(wire, true);
        this.index2Index.compareAndSwapValue(NOT_INITIALIZED, index);

        LongArrayValues index2index = getIndex2index(wire);
        newIndex(wire, index2index, 0);

        // Reset the position to its original value.
        if (!writePosition.compareAndSwapValue(oldPos, 0))
            throw new IllegalStateException("Who reset the position?");
    }

    /**
     * Retrieves the {@link LongArrayValues} for the index2index array. If the index2index array
     * has not been initialized, it reads it from the provided {@link Wire}.
     *
     * @param wire The wire object used to read from the queue.
     * @return The {@link LongArrayValues} representing the index2index array.
     */
    @SuppressWarnings("try")
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
    private long getSecondaryAddress(@NotNull Wire wire, @NotNull LongArrayValues index2indexArr, int index2) throws StreamCorruptedException {
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
            throwNumEntriesExceededForRollCycle(sequenceNumber);
        }
        long secondaryAddress = getSecondaryAddress(wire, index2indexArr, index2);
        if (secondaryAddress > bytes.capacity())
            throwSecondaryAddressError(secondaryAddress);
        bytes.readLimitToCapacity();
        LongArrayValues indexValues = arrayForAddress(wire, secondaryAddress);
        int index3 = (int) ((sequenceNumber >>> indexSpacingBits) & (indexCount - 1));

        // check the last one first.
        long posN = indexValues.getValueAt(index3);
        if (posN == 0) {
            indexValues.setValueAt(index3, position);
            indexValues.setMaxUsed(index3 + 1L);
        } else {
            indexValues.setValueAt(index3, position);
            return;
        }
        nextEntryToBeIndexed.setMaxValue(sequenceNumber + indexSpacing);
    }

    /**
     * Throws an {@link IllegalStateException} if an invalid secondary address is encountered.
     *
     * @param secondaryAddress The secondary address that caused the error.
     */
    private void throwSecondaryAddressError(long secondaryAddress) {
        throw new IllegalStateException("sa2: " + secondaryAddress);
    }

    /**
     * Throws an {@link IllegalStateException} when the sequence number exceeds the allowed maximum
     * number of entries for the current roll cycle.
     *
     * @param sequenceNumber The sequence number that exceeds the roll cycle's entry limit.
     */
    private void throwNumEntriesExceededForRollCycle(long sequenceNumber) {
        throw new IllegalStateException("Unable to index " + sequenceNumber + ", the number of entries exceeds max number for the current rollcycle");
    }

    /**
     * Determines if the given index is indexable based on the current index spacing.
     * An index is indexable if it aligns with the defined index spacing.
     *
     * @param index The index to check for indexability.
     * @return {@code true} if the index is indexable, otherwise {@code false}.
     */
    @Override
    public boolean indexable(long index) {
        throwExceptionIfClosed();

        return (index & (indexSpacing - 1)) == 0;
    }

    /**
     * Retrieves the last sequence number in the queue.
     * This method attempts to retrieve the sequence number based on the current write position and may use
     * a linear scan if the exact sequence is not readily available.
     *
     * @param ec The {@link ExcerptContext} used to navigate the queue.
     * @return The last sequence number, or {@code -1} if it cannot be found.
     * @throws StreamCorruptedException If the sequence cannot be determined due to corruption.
     */
    @Override
    public long lastSequenceNumber(@NotNull ExcerptContext ec)
            throws StreamCorruptedException {
        throwExceptionIfClosed();

        Sequence sequence1 = this.sequence;
        if (sequence1 != null) {
            for (int i = 0; i < 128; i++) {

                long address = writePosition.getVolatileValue(0);
                if (address == 0)
                    return -1;
                long sequence = sequence1.getSequence(address);
                if (sequence == Sequence.NOT_FOUND_RETRY)
                    continue;
                if (sequence == Sequence.NOT_FOUND)
                    break;
                try {
                    Wire wireForIndex = ec.wireForIndex();
                    return wireForIndex == null ? sequence : linearScanByPosition(wireForIndex, Long.MAX_VALUE, sequence, address, true);
                } catch (EOFException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        return sequenceForPosition(ec, Long.MAX_VALUE, false);
    }

    /**
     * Returns the number of indices available in the current roll cycle.
     *
     * @return The number of indices.
     */
    @Override
    public int indexCount() {
        return indexCount;
    }

    /**
     * Returns the spacing between indexed entries.
     * Index spacing defines how frequently entries are indexed.
     *
     * @return The index spacing.
     */
    @Override
    public int indexSpacing() {
        return indexSpacing;
    }

    /**
     * Moves to the end of the wire, scanning for the final sequence.
     * This method attempts to locate the last written entry in the wire and updates the sequence accordingly.
     *
     * @param wire The {@link Wire} object used to navigate the queue.
     * @return The last sequence number, or {@code -1} if it cannot be determined.
     */
    public long moveToEnd(final Wire wire) {
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

                // Iterate through the wire to find the last complete entry.
                for (; ; ) {
                    int header = bytes.readVolatileInt(endAddress);
                    if (header == 0 || Wires.isNotComplete(header))
                        return sequence;

                    int len = Wires.lengthOf(header) + 4;
                    len += (int) BytesUtil.padOffset(len);

                    bytes.readSkip(len);
                    endAddress += len;

                    if (Wires.isData(header))
                        sequence += 1;

                }
            }
        }
        return -1;
    }

    /**
     * Returns the count of linear scans performed during indexing.
     *
     * @return The count of linear scans.
     */
    @Override
    public int linearScanCount() {
        return linearScanCount;
    }

    /**
     * Returns the count of linear scans by position performed during indexing.
     *
     * @return The count of linear scans by position.
     */
    @Override
    public int linearScanByPositionCount() {
        return linearScanByPositionCount;
    }

    /**
     * Enumeration of fields used in the indexing structure.
     * This defines the keys used for reading and writing index-related data in the wire.
     */
    enum IndexingFields implements WireKey {
        indexCount, indexSpacing, index2Index,
        lastIndex // NOTE: the nextEntryToBeIndexed
    }

    /**
     * Holder class for {@link LongArrayValues} that caches the address and provides efficient access.
     * This class is used to cache the address of an array and its values for quick lookups in the indexing structure.
     */
    static class LongArrayValuesHolder {
        private final LongArrayValues values;
        private long address;

        /**
         * Constructs a holder for the provided {@link LongArrayValues}.
         *
         * @param values The {@link LongArrayValues} to hold.
         */
        LongArrayValuesHolder(LongArrayValues values) {
            this.values = values;
            address = Long.MIN_VALUE;
        }

        /**
         * Gets the current address of the held values.
         *
         * @return The address.
         */
        public long address() {
            return address;
        }

        /**
         * Sets a new address for the held values.
         *
         * @param address The new address.
         */
        public void address(long address) {
            this.address = address;
        }

        /**
         * Returns the {@link LongArrayValues} held by this holder.
         *
         * @return The {@link LongArrayValues}.
         */
        public LongArrayValues values() {
            return values;
        }
    }
}
