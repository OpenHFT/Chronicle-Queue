/*
 * Copyright 2016-2020 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.core.values.TwoLongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The SingleChronicleQueueStore class represents a store for Chronicle Queue.
 * It is responsible for maintaining the indexed write position, sequence, and mapping
 * the file to memory using {@link MappedBytes}. It handles reading and writing data in
 * a queue and supports efficient roll cycles and indexing.
 */
public class SingleChronicleQueueStore extends AbstractCloseable implements WireStore {

    static {
        // Register SCQIndexing with the class alias pool for serialization purposes
        ClassAliasPool.CLASS_ALIASES.addAlias(SCQIndexing.class);
    }

    @NotNull
    final SCQIndexing indexing; // The indexing structure for the store
    @NotNull
    private final LongValue writePosition; // The current write position in the queue
    @NotNull
    private final MappedBytes mappedBytes; // The mapped bytes for the queue store
    @NotNull
    private final MappedFile mappedFile; // The mapped file associated with the store
    private final int dataVersion; // The version of the data format
    @SuppressWarnings("deprecation")
    @NotNull
    private final transient Sequence sequence; // The sequence for encoding roll cycles

    private int cycle; // The current cycle of the queue

    /**
     * Constructor used by {@link net.openhft.chronicle.wire.Demarshallable} to create an instance of SingleChronicleQueueStore
     * from a wire input. This constructor is used during deserialization.
     *
     * @param wire the wire input to read from
     */
    @UsedViaReflection
    @SuppressWarnings("this-escape")
    private SingleChronicleQueueStore(@NotNull WireIn wire) {
        boolean failed = true;

        try {
            // Load the write position from the wire input
            writePosition = loadWritePosition(wire);
            this.mappedBytes = (MappedBytes) wire.bytes();
            this.mappedFile = mappedBytes.mappedFile();
            mappedFile.reserve(this);
            // Initialize indexing structure from wire
            this.indexing = Objects.requireNonNull(wire.read(MetaDataField.indexing).typedMarshallable());
            this.indexing.writePosition = writePosition;
            // Initialize the sequence for roll cycle encoding
            this.sequence = new RollCycleEncodeSequence(writePosition, indexing.indexCount(), indexing.indexSpacing());
            this.indexing.sequence = sequence;

            // Handle data versioning for backward compatibility
            final String fieldName = wire.readEvent(String.class);
            int version = 0;

            // Should cover fieldless and normal binary
            if (fieldName == null || MetaDataField.dataFormat.name().equals(fieldName))
                version = wire.getValueIn().int32();
            else
                Jvm.warn().on(getClass(), "Unexpected field " + fieldName);
            this.dataVersion = version > 1 ? 0 : version;

            singleThreadedCheckDisabled(true);
            failed = false;
        } finally {
            if (failed)
                close(); // Ensure resources are closed on failure
        }
    }

    /**
     * Constructs a new SingleChronicleQueueStore instance.
     * This constructor is used for creating a new queue store from scratch.
     *
     * @param rollCycle    the roll cycle configuration for the store
     * @param wireType     the wire type used for serialization
     * @param mappedBytes  the mapped bytes for memory mapping the data store file
     * @param indexCount   the number of entries in each index
     * @param indexSpacing the spacing between indexed entries
     */
    @SuppressWarnings("this-escape")
    public SingleChronicleQueueStore(@NotNull RollCycle rollCycle,
                                     @NotNull final WireType wireType,
                                     @NotNull MappedBytes mappedBytes,
                                     int indexCount,
                                     int indexSpacing) {
        this.mappedBytes = mappedBytes;
        this.mappedFile = mappedBytes.mappedFile();
        mappedFile.reserve(this);

        // Ensure index count and spacing are powers of 2
        indexCount = Maths.nextPower2(indexCount, 8);
        indexSpacing = Maths.nextPower2(indexSpacing, 1);

        // Initialize the indexing and sequence structures
        this.indexing = new SCQIndexing(wireType, indexCount, indexSpacing);
        this.indexing.writePosition = this.writePosition = wireType.newTwoLongReference().get();
        this.indexing.sequence = this.sequence = new RollCycleEncodeSequence(writePosition,
                rollCycle.defaultIndexCount(),
                rollCycle.defaultIndexSpacing());
        this.dataVersion = 1;

        singleThreadedCheckDisabled(true);
    }

    /**
     * Dumps the contents of a Chronicle Queue directory to a string for debugging.
     *
     * @param directoryFilePath the path to the directory to dump
     * @return the dumped contents as a string
     */
    @NotNull
    public static String dump(@NotNull String directoryFilePath) {
        return ChronicleQueue.singleBuilder(directoryFilePath).build().dump();
    }

    /**
     * Binds a value to the wire output for either int64 or int128, depending on the value type.
     *
     * @param wireOut the wire output to write to
     * @param value   the value to bind
     * @return the updated wire output
     */
    private static WireOut intForBinding(ValueOut wireOut, final LongValue value) {
        return value instanceof TwoLongValue ?
                wireOut.int128forBinding(0L, 0L, (TwoLongValue) value) :
                wireOut.int64forBinding(0L, value);

    }

    /**
     * Loads the write position from the wire input. Depending on the encoding, this could
     * be a single long value or an array of two long values.
     *
     * @param wire the wire input to read from
     * @return the loaded write position
     */
    private LongValue loadWritePosition(@NotNull WireIn wire) {

        final ValueIn read = wire.read(MetaDataField.writePosition);

        final int code;
        final long start = wire.bytes().readPosition();

        try {
            wire.consumePadding();
            code = wire.bytes().uncheckedReadUnsignedByte();
        } finally {
            wire.bytes().readPosition(start);
        }

        // Check if the write position is encoded as a two-long array
        if (code == BinaryWireCode.I64_ARRAY) {
            TwoLongValue result = wire.newTwoLongReference();
            // when the write position is and array it also encodes the sequence number in the write position as the second long value
            read.int128(result);
            return result;
        }

        // Otherwise, treat it as a single long value
        final LongValue result = wire.newLongReference();
        read.int64(result);
        return result;

    }

    /**
     * Returns the file associated with the store.
     *
     * @return the file used by the queue store
     */
    @NotNull
    @Override
    public File file() {
        return mappedFile.file();
    }

    /**
     * Dumps the contents of the queue using the specified {@link WireType}.
     * This method provides a detailed string representation of the queue's contents.
     *
     * @param wireType the {@link WireType} used to interpret the contents of the queue
     * @return a string representing the dumped contents of the queue
     */
    @Override
    public String dump(WireType wireType) {
        return dump(wireType, false);
    }

    /**
     * Dumps the contents of the queue using the specified {@link WireType}, with an option to abbreviate.
     *
     * @param wireType the {@link WireType} used to interpret the contents of the queue
     * @param abbrev   whether to abbreviate the dumped contents
     * @return a string representing the dumped contents of the queue
     */
    private String dump(WireType wireType, boolean abbrev) {
        try (MappedBytes bytes = MappedBytes.mappedBytes(mappedFile)) {
            bytes.readLimit(bytes.realCapacity()); // Set the read limit to the actual capacity of the file
            final Wire w = wireType.apply(bytes); // Apply the wire type to the bytes
            w.usePadding(dataVersion > 0); // Use padding if data version > 0
            return Wires.fromSizePrefixedBlobs(w, abbrev); // Return the queue contents as size-prefixed blobs
        }
    }

    /**
     * Dumps the header of the queue. This is useful for examining metadata about the queue.
     *
     * @return a string representing the dumped header contents of the queue
     */
    @Override
    public String dumpHeader() {
        try (MappedBytes bytes = MappedBytes.mappedBytes(mappedFile)) {
            int size = bytes.readInt(0); // Read the header size at position 0
            if (!Wires.isReady(size))
                return "not ready"; // If the header is not ready, return "not ready"
            bytes.readLimit(Wires.lengthOf(size) + 4L); // Set the read limit to the length of the header
            return Wires.fromSizePrefixedBlobs(bytes); // Dump the size-prefixed blobs
        }
    }

    /**
     * Retrieves the current write position in the queue.
     *
     * @return the current volatile write position
     */
    @Override
    public long writePosition() {
        return this.writePosition.getVolatileValue();
    }

    /**
     * Sets the write position to the specified value. Ensures that the position is valid.
     *
     * @param position the new write position to set
     * @return the current instance of {@link WireStore}
     */
    @NotNull
    @Override
    public WireStore writePosition(long position) {
        throwExceptionIfClosed(); // Ensure the store is not closed

        assert writePosition.getVolatileValue() + mappedFile.chunkSize() > position; // Assert that the position is within valid bounds
        writePosition.setMaxValue(position); // Update the write position to the new value
        return this;
    }

    /**
     * Moves the excerpt context to the specified index for reading.
     *
     * @param ec    the excerpt context to navigate
     * @param index the index to move to for reading
     * @return the result of the scan, indicating whether the index was found
     */
    @Nullable
    @Override
    public ScanResult moveToIndexForRead(@NotNull ExcerptContext ec, long index) {
        throwExceptionIfClosed(); // Ensure the store is not closed

        try {
            return indexing.moveToIndex(ec, index); // Attempt to move to the specified index using the indexing structure
        } catch (@NotNull UnrecoverableTimeoutException e) {
            return ScanResult.NOT_REACHED; // If an unrecoverable timeout occurs, return "NOT_REACHED"
        }
    }

    /**
     * Moves the excerpt context to the start of the queue for reading.
     *
     * @param ec the excerpt context to navigate
     * @return the result of the scan, indicating whether the start was found
     */
    @Nullable
    @Override
    public ScanResult moveToStartForRead(@NotNull ExcerptContext ec) {
        throwExceptionIfClosed(); // Ensure the store is not closed

        Wire wire = ec.wire(); // Get the wire associated with the excerpt context
        wire.bytes().readPositionUnlimited(0); // Set the read position to the start of the queue

        try {
            // Attempt to read the data header and determine the type
            final WireIn.HeaderType headerType = wire.readDataHeader(true);
            switch (headerType) {
                case DATA:
                case META_DATA:
                    return ScanResult.FOUND; // If the header is data or metadata, return "FOUND"
                case NONE:
                    return ScanResult.NOT_REACHED; // If no data is found, return "NOT_REACHED"
                case EOF:
                    return ScanResult.END_OF_FILE; // If EOF is reached, return "END_OF_FILE"
                default:
                    throw new AssertionError("headerType=" + headerType); // Unknown header type, throw an assertion error
            }
        } catch (EOFException eof) {
            return ScanResult.END_OF_FILE; // If an EOFException is thrown, return "END_OF_FILE"
        }
    }

    /**
     * Moves the wire to the end of the queue for reading.
     *
     * @param w the wire to navigate
     * @return the index at the end of the queue
     */
    @Override
    public long moveToEndForRead(@NotNull Wire w) {
        throwExceptionIfClosed(); // Ensure the store is not closed

        return indexing.moveToEnd(w); // Move to the end of the queue using the indexing structure
    }

    /**
     * Handles the cleanup and closure of resources, ensuring that write position, indexing, and mapped file resources
     * are properly released. This method is called when the store is being closed.
     */
    @Override
    protected void performClose() {
        // Safely close the write position and indexing resources
        Closeable.closeQuietly(writePosition);
        Closeable.closeQuietly(indexing);

        // Release the mapped bytes and file if they are initialized
        if (mappedBytes != null) {
            mappedBytes.release(INIT); // Release the initial reference to mapped bytes
            try {
                mappedFile.release(this); // Release the mapped file
            } catch (IllegalStateException e) {
                Jvm.warn().on(getClass(), "trouble releasing " + mappedFile, e); // Log any issues during release
            }
        }
    }

    /**
     * Creates and returns a new instance of {@link MappedBytes}. Each instance of the tailer or appender
     * can be at different positions, so a fresh instance is returned.
     *
     * @return a new instance of {@link MappedBytes} for the mapped file
     */
    @NotNull
    @Override
    public MappedBytes bytes() {
        throwExceptionIfClosed(); // Ensure the store is not closed

        final MappedBytes mbytes = MappedBytes.mappedBytes(mappedFile); // Create a new MappedBytes instance
        mbytes.singleThreadedCheckDisabled(true); // Disable single-threaded checks for performance
        return mbytes;
    }

    /**
     * Retrieves the sequence number corresponding to the given position in the {@link ExcerptContext}.
     *
     * @param ec        the excerpt context for reading
     * @param position  the position to find the sequence number for
     * @param inclusive whether the position should be inclusive
     * @return the sequence number at the given position
     * @throws StreamCorruptedException if the stream is corrupted
     */
    @Override
    public long sequenceForPosition(@NotNull final ExcerptContext ec, final long position, boolean inclusive) throws StreamCorruptedException {
        throwExceptionIfClosed(); // Ensure the store is not closed

        return indexing.sequenceForPosition(ec, position, inclusive); // Retrieve the sequence number from indexing
    }

    /**
     * Retrieves the last sequence number in the given {@link ExcerptContext}.
     *
     * @param ec the excerpt context for reading
     * @return the last sequence number
     * @throws StreamCorruptedException if the stream is corrupted
     */
    public long lastSequenceNumber(@NotNull ExcerptContext ec) throws StreamCorruptedException {
        throwExceptionIfClosedInSetter(); // Ensure the store is not closed during a setter operation
        return indexing.lastSequenceNumber(ec); // Retrieve the last sequence number from indexing
    }

    /**
     * Returns a string representation of the store's current state, including details about indexing,
     * write position, mapped file, and whether the store is closed.
     *
     * @return a string representation of the store
     */
    @NotNull
    @Override
    public String toString() {
        return "SingleChronicleQueueStore{" +
                "indexing=" + indexing +
                ", writePosition/seq=" + writePosition +
                ", mappedFile=" + mappedFile +
                ", isClosed=" + isClosed() +
                '}';
    }

    // *************************************************************************
    // Marshalling
    // *************************************************************************

    /**
     * Writes the current state of the store to the given {@link WireOut}.
     * This includes the write position, indexing, and data format version.
     *
     * @param wire the wire to write to
     */
    @Override
    public void writeMarshallable(@NotNull WireOut wire) {

        ValueOut wireOut = wire.write(MetaDataField.writePosition);
        intForBinding(wireOut, writePosition) // Write the write position
                .write(MetaDataField.indexing).typedMarshallable(this.indexing) // Write the indexing
                .write(MetaDataField.dataFormat).int32(dataVersion); // Write the data format version
    }

    /**
     * Initializes the index in the given {@link Wire}.
     *
     * @param wire the wire to initialize the index in
     */
    @Override
    public void initIndex(@NotNull Wire wire) {
        throwExceptionIfClosedInSetter(); // Ensure the store is not closed during a setter operation

        try {
            indexing.initIndex(wire); // Initialize the index using the indexing structure
        } catch (IOException ex) {
            throw new UncheckedIOException(ex); // Wrap IOExceptions in UncheckedIOException
        }
    }

    /**
     * Determines if the given index is valid and can be indexed.
     *
     * @param index the index to check
     * @return true if the index is indexable, false otherwise
     */
    @Override
    public boolean indexable(long index) {
        return indexing.indexable(index); // Check if the index can be indexed
    }

    /**
     * Sets the position for the given sequence number in the {@link ExcerptContext}.
     *
     * @param ec             the excerpt context for reading
     * @param sequenceNumber the sequence number to set the position for
     * @param position       the position to set
     * @throws StreamCorruptedException if the stream is corrupted
     */
    @Override
    public void setPositionForSequenceNumber(@NotNull final ExcerptContext ec,
                                             long sequenceNumber,
                                             long position) throws StreamCorruptedException {
        throwExceptionIfClosedInSetter(); // Ensure the store is not closed during a setter operation

        sequence.setSequence(sequenceNumber, position); // Set the sequence number and position

        long nextSequence = indexing.nextEntryToBeIndexed(); // Get the next entry to be indexed
        if (nextSequence > sequenceNumber)
            return; // If the next sequence is already greater, no need to update

        indexing.setPositionForSequenceNumber(ec, sequenceNumber, position); // Set the position in the indexing structure
    }

    /**
     * Performs a linear scan from a known index to another index in the given {@link ExcerptContext}.
     *
     * @param index        the target index
     * @param knownIndex   the known starting index
     * @param ec           the excerpt context for reading
     * @param knownAddress the known address to start scanning from
     * @return the result of the scan
     */
    @Override
    public ScanResult linearScanTo(final long index, final long knownIndex, final ExcerptContext ec, final long knownAddress) {
        throwExceptionIfClosed(); // Ensure the store is not closed

        return indexing.linearScanTo(index, knownIndex, ec, knownAddress); // Perform the linear scan using the indexing structure
    }

    /**
     * Writes an end-of-file (EOF) marker in the given {@link Wire} and attempts to shrink the file.
     *
     * @param wire      the wire to write the EOF marker to
     * @param timeoutMS the timeout for writing the EOF marker
     * @return true if the EOF marker was written successfully, false otherwise
     */
    @Override
    public boolean writeEOF(@NotNull Wire wire, long timeoutMS) {
        throwExceptionIfClosed(); // Ensure the store is not closed

        String fileName = mappedFile.file().getAbsolutePath(); // Get the name of the mapped file

        // Attempt to write the EOF marker, and ensure resources are released afterward
        if (wire.bytes().tryReserve(this)) {
            try {
                return writeEOFAndShrink(wire, timeoutMS); // Write the EOF marker and shrink the file
            } finally {
                wire.bytes().release(this);
            }
        }

        // If unable to reserve bytes, create a new instance of MappedBytes and try again
        try (MappedBytes bytes = MappedBytes.mappedBytes(mappedFile.file(), mappedFile.chunkSize())) {
            Wire wire0 = WireType.valueOf(wire).apply(bytes);
            return writeEOFAndShrink(wire0, timeoutMS);

        } catch (Exception e) {
            Jvm.warn().on(getClass(), "unable to write the EOF file=" + fileName, e);
            return false;
        }
    }

    /**
     * Writes an end-of-file (EOF) marker to the wire and shrinks the file if necessary.
     *
     * @param wire      the wire to write the EOF marker to
     * @param timeoutMS the timeout for writing the EOF marker
     * @return true if the EOF marker was written successfully, false otherwise
     */
    boolean writeEOFAndShrink(@NotNull Wire wire, long timeoutMS) {
        return wire.writeEndOfWire(timeoutMS, TimeUnit.MILLISECONDS, writePosition()); // Write the EOF marker
    }

    /**
     * Returns the data format version used by this store.
     *
     * @return the data format version
     */
    @Override
    public int dataVersion() {
        return dataVersion;
    }

    /**
     * Sets the cycle for this store.
     *
     * @param cycle the cycle to set
     * @return the updated {@link SingleChronicleQueueStore}
     */
    public SingleChronicleQueueStore cycle(int cycle) {
        throwExceptionIfClosedInSetter(); // Ensure the store is not closed during a setter operation

        this.cycle = cycle;
        return this;
    }

    /**
     * Returns the current cycle of the store.
     *
     * @return the current cycle
     */
    public int cycle() {
        return cycle;
    }

    /**
     * Returns the current file being used by this store.
     *
     * @return the file being used by this store
     */
    public File currentFile() {
        return mappedFile.file();
    }
}
