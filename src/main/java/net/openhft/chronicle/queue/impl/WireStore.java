/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.impl.single.ScanResult;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.StreamCorruptedException;

/**
 * The {@code WireStore} interface represents a store for wire-level data that supports
 * reading, writing, and indexing of excerpts. It extends {@link CommonStore} and {@link Closeable}
 * to provide methods for managing the position, indexing, and scanning of stored data.
 */
public interface WireStore extends CommonStore, Closeable {

    /**
     * Sets the write position of the store, marking the position of the last written excerpt.
     *
     * @param position the position of the last written excerpt
     * @return this store
     */
    @NotNull
    WireStore writePosition(long position);

    /**
     * Retrieves the write position, which marks the start of the last written excerpt in the store.
     *
     * @return the start of the last written excerpt
     */
    long writePosition();

    /**
     * Moves to the index for reading based on the provided {@code index}.
     *
     * @param ec    the {@link ExcerptContext} for managing the wire and bytes during the operation
     * @param index the index to move to
     * @return {@link ScanResult} indicating the result of the move operation
     */
    @Nullable
    ScanResult moveToIndexForRead(@NotNull ExcerptContext ec, long index);

    /**
     * Moves to the start of the store for reading.
     *
     * @param ec the {@link ExcerptContext} to use for the operation
     * @return {@link ScanResult} indicating the result of the move operation
     */
    @Nullable
    ScanResult moveToStartForRead(@NotNull ExcerptContext ec);

    /**
     * Retrieves the sequence number corresponding to a given position in the store.
     *
     * @param ec        the {@link ExcerptContext} managing the wire and bytes
     * @param position  the position of the start of the message
     * @param inclusive if {@code true}, the position is included in the scan
     * @return the index corresponding to the position
     * @throws UnrecoverableTimeoutException if the operation times out
     * @throws StreamCorruptedException      if the stream is corrupted
     */
    long sequenceForPosition(ExcerptContext ec, long position, boolean inclusive) throws StreamCorruptedException;

    /**
     * Sets the position using the provided parameters.
     *
     * @param ec       the wire of the bytes, to work with
     * @param sequenceNumber to use when setting position
     * @param position of the start of the message
     * @throws UnrecoverableTimeoutException if the operation times out
     * @throws StreamCorruptedException if the stream is corrupted
     */
    void setPositionForSequenceNumber(final ExcerptContext ec, long sequenceNumber, long position) throws StreamCorruptedException;

    /**
     * Writes an EOF (End Of File) marker to the store.
     *
     * @param wire      the {@link Wire} to use for writing the EOF
     * @param timeoutMS the timeout for the write operation in milliseconds
     * @return {@code true} if the EOF was successfully written, {@code false} otherwise
     */
    boolean writeEOF(Wire wire, long timeoutMS);

    /**
     * Checks if the given index is indexable.
     *
     * @param index the index to check
     * @return {@code true} if the index is indexable, {@code false} otherwise
     */
    boolean indexable(long index);

    /**
     * Performs a linear scan to the specified index, starting from a known index and known address.
     *
     * @param index        the index to scan to
     * @param knownIndex   a known starting index
     * @param ec           the {@link ExcerptContext} for managing the wire and bytes
     * @param knownAddress the known address to start scanning from
     * @return {@link ScanResult} indicating the result of the scan
     */
    ScanResult linearScanTo(long index, long knownIndex, ExcerptContext ec, long knownAddress);

    /**
     * Moves to the end of the store for reading.
     *
     * @param w the {@link Wire} to use for the operation
     * @return the position at the end of the store
     */
    long moveToEndForRead(@NotNull Wire w);

    /**
     * Initializes the index for the provided wire.
     *
     * @param wire the {@link Wire} to initialize the index for
     */
    void initIndex(Wire wire);

    /**
     * Dumps the header of the store, typically used for debugging purposes.
     *
     * @return a string representation of the store header
     */
    String dumpHeader();

    /**
     * Retrieves the data version of the store.
     *
     * @return the data version
     */
    int dataVersion();
}
