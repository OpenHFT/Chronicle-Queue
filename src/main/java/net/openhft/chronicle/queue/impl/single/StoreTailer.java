/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MappedBytesStore;
import net.openhft.chronicle.bytes.util.DecoratedBufferUnderflowException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.PackageLocal;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import net.openhft.chronicle.core.scoped.ScopedResourcePool;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.queue.impl.single.namedtailer.IndexUpdater;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.StreamCorruptedException;
import java.text.ParseException;

import static net.openhft.chronicle.queue.TailerDirection.*;
import static net.openhft.chronicle.queue.TailerState.*;
import static net.openhft.chronicle.queue.impl.single.ScanResult.*;
import static net.openhft.chronicle.wire.NoDocumentContext.INSTANCE;
import static net.openhft.chronicle.wire.Wires.isEndOfFile;

/**
 * This class represents a StoreTailer which reads excerpts from a Chronicle Queue.
 * It is responsible for managing the current state, index, and reading direction
 * of the tailer as it processes the queue's messages. It allows for movement through
 * the queue in both forward and backward directions, ensuring efficient retrieval
 * of entries by utilizing the appropriate WireStore and cycle mechanism.
 */
class StoreTailer extends AbstractCloseable
        implements ExcerptTailer, SourceContext, ExcerptContext {
    static final int INDEXING_LINEAR_SCAN_THRESHOLD = 70;
    static final ScopedResourcePool<StringBuilder> SBP = StringBuilderPool.createThreadLocal(1);
    static final EOFException EOF_EXCEPTION = new EOFException();
    @NotNull
    private final SingleChronicleQueue queue;
    private final WireStorePool storePool;
    private final IndexUpdater indexUpdater;
    private final StoreTailerContext context = new StoreTailerContext();
    private final MoveToState moveToState = new MoveToState();
    private final Finalizer finalizer;
    long index; // index of the next read.
    long lastReadIndex; // index of the last read message
    @Nullable
    SingleChronicleQueueStore store;
    private int cycle;
    private TailerDirection direction = TailerDirection.FORWARD;
    private Wire wireForIndex;
    private boolean readAfterReplicaAcknowledged;
    @NotNull
    private TailerState state = UNINITIALISED;
    private long indexAtCreation = Long.MIN_VALUE;
    private boolean readingDocumentFound = false;
    private boolean striding = false;

    /**
     * Constructs a StoreTailer to read from the provided queue with the given store pool.
     *
     * @param queue     the queue to be read
     * @param storePool the pool that manages WireStore instances
     */
    public StoreTailer(@NotNull final SingleChronicleQueue queue, WireStorePool storePool) {
        this(queue, storePool, null);
    }

    /**
     * Constructs a StoreTailer with the option to provide an index updater.
     *
     * @param queue        the queue to be read
     * @param storePool    the pool that manages WireStore instances
     * @param indexUpdater optional index updater for reading from specific positions
     */
    public StoreTailer(@NotNull final SingleChronicleQueue queue, WireStorePool storePool, IndexUpdater indexUpdater) {
        boolean error = true;
        try {
            this.queue = queue;
            this.storePool = storePool;
            this.indexUpdater = indexUpdater;
            this.setCycle(Integer.MIN_VALUE);
            this.index = 0;

            if (indexUpdater == null) {
                toStart();
            } else {
                moveToIndex(indexUpdater.index().getVolatileValue());
            }
            finalizer = Jvm.isResourceTracing() ? new Finalizer() : null;
            error = false;

        } finally {
            if (error)
                close();
        }

        // always put references to "this" last.
        queue.addCloseListener(this);
    }

    @Override
    public void singleThreadedCheckDisabled(boolean singleThreadedCheckDisabled) {
        final Wire privateWire = privateWire();
        if (privateWire != null) {
            privateWire.bytes().singleThreadedCheckDisabled(singleThreadedCheckDisabled);
        }
        super.singleThreadedCheckDisabled(singleThreadedCheckDisabled);
    }

    @Override
    public void singleThreadedCheckReset() {
        super.singleThreadedCheckReset();
        final Wire privateWire = privateWire();
        if (privateWire != null) {
            privateWire.bytes().singleThreadedCheckReset();
        }
    }

    /**
     * Reads a document using the provided marshallable reader. If the document is available,
     * it is read and the reader is used to process the document's content.
     *
     * @param reader the marshallable reader to process the document's content
     * @return true if a document was read, false otherwise
     */
    @Override
    public boolean readDocument(@NotNull final ReadMarshallable reader) {
        throwExceptionIfClosed();

        try (@NotNull DocumentContext dc = readingDocument(false)) {
            if (!dc.isPresent())
                return false;
            reader.readMarshallable(dc.wire());
        }
        return true;
    }

    /**
     * Retrieves a {@link DocumentContext} for reading the next document.
     *
     * @return the document context for reading the next document, or an empty one if no document is present
     */
    @Override
    @NotNull
    public DocumentContext readingDocument() {
        // trying to create an initial document without a direction should not consume a message
        final long index = index();
        if (direction == NONE && (index == indexAtCreation || index == 0) && !readingDocumentFound) {
            return INSTANCE;
        }
        return readingDocument(false);
    }

    /**
     * Closes the tailer and releases resources like the index updater and wire.
     */
    @Override
    protected void performClose() {
        Closeable.closeQuietly(indexUpdater);
        // the wire ref count will be released here by setting it to null
        context.wire(null);
        final Wire w0 = wireForIndex;
        if (w0 != null)
            w0.bytes().release(INIT);
        wireForIndex = null;
        releaseStore();
    }

    @Override
    public Wire wire() {
        throwExceptionIfClosed();

        return privateWire();
    }

    /**
     * Retrieves the private wire associated with the tailer.
     * This wire is the one used by the tailer's context for reading excerpts.
     *
     * @return The wire used by the tailer's context, or null if none is available
     */
    public @Nullable Wire privateWire() {
        return context.wire();
    }

    @Override
    public Wire wireForIndex() {
        throwExceptionIfClosed();

        return wireForIndex;
    }

    @Override
    public long timeoutMS() {
        return queue.timeoutMS;
    }

    @Override
    public int sourceId() {
        return queue.sourceId;
    }

    /**
     * Provides a string representation of the StoreTailer, showing its current state.
     *
     * @return A string describing the StoreTailer, including the current index sequence, index cycle, store, and queue.
     */
    @NotNull
    @Override
    public String toString() {
        final long index = index();
        return "StoreTailer{" +
                "index sequence=" + (index == Long.MIN_VALUE ? "unset" : queue.rollCycle().toSequenceNumber(index)) +
                ", index cycle=" + (index == Long.MIN_VALUE ? "unset" : queue.rollCycle().toCycle(index)) +
                ", state=" + state +
                ", closed=" + (isClosed() ? "true" : isClosing() ? "closing" : "false") +
                ", store=" + store +
                ", queue=" + queue + '}';
    }

    /**
     * Reads a document from the queue with an option to include metadata.
     * If the document's wire has excessive remaining bytes, an AssertionError is thrown.
     *
     * @param includeMetaData Whether to include metadata in the document context
     * @return The document context for reading the document
     */
    @NotNull
    @Override
    public DocumentContext readingDocument(final boolean includeMetaData) {
        DocumentContext documentContext = readingDocument0(includeMetaData);
        // this check was added after a strange behaviour seen by one client. It should be impossible.
        if (documentContext.wire() != null)
            if (documentContext.wire().bytes().readRemaining() >= 1 << 30)
                throw new AssertionError("readRemaining " + documentContext.wire().bytes().readRemaining());
        return documentContext;
    }

    /**
     * Internal method to handle the reading of a document.
     *
     * @param includeMetaData Whether to include metadata in the document context
     * @return The document context for reading the document
     */
    DocumentContext readingDocument0(final boolean includeMetaData) {
        throwExceptionIfClosed();

        try {
            boolean next = false;
            boolean tryAgain = true;
            if (state == FOUND_IN_CYCLE) {
                try {
                    next = inACycle(includeMetaData);

                    tryAgain = false;
                } catch (EOFException eof) {
                    state = TailerState.END_OF_CYCLE;
                }
            }

            if (tryAgain)
                next = next0(includeMetaData);

            Wire wire = context.wire();
            if (wire != null && context.present(next)) {
                Bytes<?> bytes = wire.bytes();
                context.setStart(bytes.readPosition() - 4);
                readingDocumentFound = true;
                this.lastReadIndex = this.index();
                return context;
            }

            readingDocumentCycleNotFound(next);

        } catch (StreamCorruptedException e) {
            throw new IllegalStateException(e);
        } catch (UnrecoverableTimeoutException notComplete) {
            // Treat the document as empty if the operation timed out.
        } catch (DecoratedBufferUnderflowException e) {
            // read-only tailer view is fixed, a writer could continue past the end of the view
            // at the point this tailer was created. Log a warning and return no document.
            readingDocumentDBUE(e);
        }
        return INSTANCE;
    }

    /**
     * Handles a buffer underflow exception during document reading.
     * Logs a warning for read-only queues, otherwise rethrows the exception.
     *
     * @param e The buffer underflow exception encountered during reading
     */
    private void readingDocumentDBUE(DecoratedBufferUnderflowException e) {
        if (queue.isReadOnly()) {
            Jvm.warn().on(StoreTailer.class,
                    "Tried to read past the end of a read-only view. " +
                            "Underlying data store may have grown since this tailer was created.", e);
        } else {
            throw e;
        }
    }

    /**
     * Handles the case where the current cycle is not found during document reading.
     *
     * @param next Indicates whether there are more entries to read
     */
    private void readingDocumentCycleNotFound(boolean next) {
        RollCycle rollCycle = queue.rollCycle();
        if (state == CYCLE_NOT_FOUND && direction == FORWARD) {
            int firstCycle = queue.firstCycle();
            if (rollCycle.toCycle(index()) < firstCycle)
                toStart();
        } else if (!next && state == CYCLE_NOT_FOUND && cycle != queue.cycle()) {
            // appenders have moved on, it's possible that linearScan is hitting EOF, which is ignored
            // since we can't find an entry at current index, indicate that we're at the end of a cycle
            state = TailerState.END_OF_CYCLE;
        }
    }

    /**
     * Attempts to move to the next document in the queue, taking into account the current state.
     * This method handles various states such as uninitialised, not reached in cycle, found in cycle, and more.
     * If unable to progress through the cycles after several attempts, an exception is thrown.
     *
     * @param includeMetaData Whether metadata should be included in the document
     * @return true if a document is found, otherwise false
     * @throws StreamCorruptedException      If the wire data is corrupted
     * @throws UnrecoverableTimeoutException If the operation times out while trying to access the queue
     */
    private boolean next0(final boolean includeMetaData) throws StreamCorruptedException {
        context.metaData(false);
        for (int i = 0; i < 1000; i++) {
            switch (state) {
                case UNINITIALISED:
                    final long firstIndex = queue.firstIndex();
                    if (firstIndex == Long.MAX_VALUE)
                        return false;
                    if (includeMetaData) {
                        if (moveToCycle(queue.rollCycle().toCycle(firstIndex))) {
                            final Bytes<?> bytes = wire().bytes();
                            inACycleFound(bytes);
                            final int header = bytes.readInt(bytes.readPosition() - 4);
                            context.metaData(Wires.isReadyMetaData(header));
                            return true;
                        }
                    } else {
                        if (!moveToIndexInternal(firstIndex))
                            return false;
                    }
                    break;

                case NOT_REACHED_IN_CYCLE:
                    if (!moveToIndexInternal(index))
                        return false;
                    break;

                case FOUND_IN_CYCLE: {
                    try {
                        return inACycle(includeMetaData);
                    } catch (EOFException eof) {
                        state = TailerState.END_OF_CYCLE;
                    }
                    break;
                }

                case END_OF_CYCLE:
                    if (endOfCycle()) {
                        continue;
                    }

                    return false;

                case BEYOND_START_OF_CYCLE:
                    if (beyondStartOfCycle())
                        continue;
                    return false;

                case CYCLE_NOT_FOUND:
                    if (nextCycleNotFound())
                        continue;
                    return false;

                default:
                    throw new AssertionError("state=" + state);
            }
        }

        throw new IllegalStateException("Unable to progress to the next cycle, state=" + state);
    }

    /**
     * Handles the situation when the tailer reaches the end of the current cycle.
     * It tries to move to the next cycle if available.
     *
     * @return true if the next cycle was found and processed, otherwise false
     */
    private boolean endOfCycle() {
        final long oldIndex = this.index();
        final int currentCycle = queue.rollCycle().toCycle(oldIndex);
        final long nextIndex = nextIndexWithNextAvailableCycle(currentCycle);

        if (nextIndex != Long.MIN_VALUE) {
            return nextEndOfCycle(queue.rollCycle().toCycle(nextIndex));
        } else {
            state = END_OF_CYCLE;
        }
        return false;
    }

    /**
     * Handles moving beyond the start of a cycle based on the tailer's direction.
     * If the direction is forward, it resets to the uninitialised state.
     * If the direction is backward, it processes the backward movement.
     *
     * @return true if the state changes and should continue, otherwise false
     * @throws StreamCorruptedException If the wire data is corrupted
     */
    private boolean beyondStartOfCycle() throws StreamCorruptedException {
        if (direction == FORWARD) {
            state = UNINITIALISED;
            return true;
        } else if (direction == BACKWARD) {
            return beyondStartOfCycleBackward();
        }
        throw new AssertionError("direction not set, direction=" + direction);
    }

    /**
     * Moves the tailer to the next cycle, if available.
     * Updates the state accordingly based on the result of the move.
     *
     * @param nextCycle The next cycle to move to
     * @return true if the tailer successfully moved to the next cycle, otherwise false
     */
    private boolean nextEndOfCycle(final int nextCycle) {
        if (moveToCycle(nextCycle)) {
            state = FOUND_IN_CYCLE;
            return true;
        }
        if (state == END_OF_CYCLE) {
            return true;
        }
        if (state == CYCLE_NOT_FOUND) {
            return false;
        }
        if (cycle < queue.lastCycle()) {
            // Encountered an empty file without an EOF marker, marking as end of cycle
            state = END_OF_CYCLE;
            return true;
        }
        // We are here because we are waiting for an entry to be written to this file.
        // Winding back to the previous cycle results in a re-initialisation of all the objects => garbage
        cycle(nextCycle);
        state = CYCLE_NOT_FOUND;
        return false;
    }

    /**
     * Retrieves the index of the last message read by this tailer.
     *
     * @return The last read index as a long value
     */
    @Override
    public long lastReadIndex() {
        return this.lastReadIndex;
    }

    /**
     * Handles moving beyond the start of a cycle when the direction is backward.
     * This method checks the position of the last entry and attempts to move to the last available cycle.
     *
     * @return true if successfully moved beyond the start of the cycle, otherwise false
     * @throws StreamCorruptedException If the wire data is corrupted
     */
    private boolean beyondStartOfCycleBackward() throws StreamCorruptedException {
        // give the position of the last entry and
        // flag we want to count it even though we don't know if it will be meta data or not.

        // The first index was zero, and we've run past it to -1
        if (index < 0)
            return false;

        final boolean foundCycle = cycle(queue.rollCycle().toCycle(index()));

        if (foundCycle && this.cycle >= 0) {
            final long lastSequenceNumberInThisCycle = store().sequenceForPosition(this, Long.MAX_VALUE, false);
            final long nextIndex = queue.rollCycle().toIndex(this.cycle, lastSequenceNumberInThisCycle);
            moveToIndexInternal(nextIndex);
            state = FOUND_IN_CYCLE;
            return true;
        }

        final int cycle = queue.rollCycle().toCycle(index());
        final long nextIndex = nextIndexWithNextAvailableCycle(cycle);

        if (nextIndex != Long.MIN_VALUE) {
            moveToIndexInternal(nextIndex);
            state = FOUND_IN_CYCLE;
            return true;
        }

        state = BEYOND_START_OF_CYCLE;
        return false;
    }

    /**
     * Handles the situation when the next cycle is not found.
     *
     * @return true if the next cycle was found and successfully moved to, otherwise false
     */
    private boolean nextCycleNotFound() {
        if (index() == Long.MIN_VALUE) {
            if (this.store != null)
                queue.closeStore(this.store);
            this.store = null;
            return false;
        }

        if (moveToIndexInternal(index())) {
            state = FOUND_IN_CYCLE;
            return true;
        }
        return false;
    }

    /**
     * Processes the current cycle and handles different scenarios such as replication acknowledgment and direction.
     *
     * @param includeMetaData Whether metadata should be included in the document
     * @return true if successfully processed the current cycle, otherwise false
     * @throws EOFException If the end of the file is reached
     */
    private boolean inACycle(final boolean includeMetaData) throws EOFException {
        if (readAfterReplicaAcknowledged && inACycleCheckRep()) return false;

        if (direction != TailerDirection.FORWARD && !inACycleNotForward()) return false;

        final Wire wire = privateWire();
        if (wire == null) {
            throwExceptionIfClosed();
            return false;
        }

        final Bytes<?> bytes = wire.bytes();
        return inACycle2(includeMetaData, wire, bytes);
    }

    /**
     * Processes the current cycle's data and metadata.
     *
     * @param includeMetaData Whether metadata should be included in the document
     * @param wire            The wire object representing the current cycle
     * @param bytes           The byte buffer containing the data
     * @return true if a valid document is found, otherwise false
     * @throws EOFException If the end of the file is reached
     */
    private boolean inACycle2(boolean includeMetaData, Wire wire, Bytes<?> bytes) throws EOFException {
        bytes.readLimitToCapacity();

        switch (wire.readDataHeader(includeMetaData)) {
            case NONE:
                // no more polling - appender will always write (or recover) EOF
                return false;
            case META_DATA:
                context.metaData(true);
                break;
            case DATA:
                context.metaData(false);
                break;
            case EOF:
                throw EOF_EXCEPTION;
            default:
                throw new IllegalStateException("Unknown header type");
        }

        inACycleFound(bytes);
        return true;
    }

    private AcknowledgedIndexReplicatedCheck acknowledgedIndexReplicatedCheck
            = (index, lastSequenceAck) -> index == lastSequenceAck;

    /**
     * Checks if the current index should not be read due to replication acknowledgment.
     *
     * @return true if the message should not be read, otherwise false
     */
    private boolean inACycleCheckRep() {
        final long lastSequenceAck = queue.lastAcknowledgedIndexReplicated();
        final long index = index();
        if (lastSequenceAck == -1)
            return true; // don't allow the message to be read
        if (index == Long.MIN_VALUE)
            return false; // allow the message to be read

        // check both are in the same roll cycle
        if (queue.rollCycle().toCycle(index) != queue.rollCycle().toCycle(lastSequenceAck))
            // don't allow the message to be read, until the ack has caught up with the current cycle
            // as trying to calculate the number of message between the last cycle and the current cycle
            // to too closely in terms of performance to calculate.
            return true;

        // Check if the acknowledgment matches the current index
        return !acknowledgedIndexReplicatedCheck.acknowledgedIndexReplicatedCheck(index, lastSequenceAck);
    }

    /**
     * Processes the cycle when the tailer's direction is not forward.
     * Attempts to move to the correct index or adjust accordingly if necessary.
     *
     * @return true if successfully moved to the index, otherwise false
     */
    private boolean inACycleNotForward() {
        if (!moveToIndexInternal(index())) {
            try {
                // after toEnd() call, index is past the end of the queue
                // so try to go back one (to the last record in the queue)
                if ((int) queue.rollCycle().toSequenceNumber(index()) < 0) {
                    long lastSeqNum = store().lastSequenceNumber(this);
                    if (lastSeqNum == -1) {
                        windBackCycle(cycle);
                        return moveToIndexInternal(index());
                    }

                    return moveToIndexInternal(queue.rollCycle().toIndex(cycle, lastSeqNum));
                }
                if (!moveToIndexInternal(index() - 1)) {
                    return false;
                }
            } catch (Exception e) {
                // can happen if index goes negative
                return false;
            }
        }
        return true;
    }

    /**
     * Adjusts the internal state to indicate that a message was found within the current cycle.
     * This method updates the context's read limits and sets the wire's length based on the read position.
     *
     * @param bytes The byte buffer representing the current wire's data
     */
    private void inACycleFound(@NotNull final Bytes<?> bytes) {
        context.closeReadLimit(bytes.capacity());
        privateWire().readAndSetLength(bytes.readPosition());
        final long end = bytes.readLimit();
        context.closeReadPosition(end);
    }

    /**
     * Attempts to retrieve the next index in the next available cycle. If the store file is missing,
     * the directory listing is refreshed and another attempt is made.
     *
     * @param cycle The current cycle to evaluate
     * @return The next index in the next available cycle or Long.MIN_VALUE if not found
     */
    private long nextIndexWithNextAvailableCycle(final int cycle) {
        try {
            return nextIndexWithNextAvailableCycle0(cycle);
        } catch (MissingStoreFileException e) {
            queue.refreshDirectoryListing();
            return nextIndexWithNextAvailableCycle0(cycle);
        }
    }

    /**
     * Internal logic to find the next index in the next available cycle.
     * If the next cycle is found, retrieves the next available index.
     *
     * @param cycle The current cycle to evaluate
     * @return The next index in the next available cycle or Long.MIN_VALUE if not found
     */
    private long nextIndexWithNextAvailableCycle0(final int cycle) {
        assert cycle != Integer.MIN_VALUE : "cycle == Integer.MIN_VALUE";

        if (cycle > queue.lastCycle() || direction == TailerDirection.NONE) {
            return Long.MIN_VALUE;
        }

        long nextIndex;
        final int nextCycle = cycle + direction.add();
        final boolean found = cycle(nextCycle);
        if (found)
            nextIndex = nextIndexWithinFoundCycle(nextCycle);
        else
            try {
                final int nextCycle0 = queue.nextCycle(this.cycle, direction);
                if (nextCycle0 == -1)
                    return Long.MIN_VALUE;

                if (cycle(nextCycle0)) {
                    nextIndex = nextIndexWithinFoundCycle(nextCycle0);
                } else {
                    queue.refreshDirectoryListing();
                    return Long.MIN_VALUE;
                }

            } catch (ParseException e) {
                throw new IllegalStateException(e);
            }

        if (Jvm.isResourceTracing()) {
            final int nextIndexCycle = queue.rollCycle().toCycle(nextIndex);
            if (nextIndex != Long.MIN_VALUE && nextIndexCycle - 1 != cycle) {

                /*
                 * lets say that you were using a roll cycle of TEST_SECONDLY
                 * and you wrote a message to the queue, if you created a tailer and read the first message,
                 * then waited around 22 seconds before writing the next message, when the tailer
                 * came to read the next message, there would be a gap of 22 cycle files
                 * that did not exist, that is what this is reporting. If you are using daily rolling,
                 * and writing every day, you should not see this message.
                 */

                Jvm.debug().on(getClass(), "Rolled " + (nextIndexCycle - cycle) + " " + "times to find the " +
                        "next cycle file. This can occur if your appenders have not written " +
                        "anything for a while, leaving the cycle files with a gap.");
            }
        }

        return nextIndex;
    }

    /**
     * Determines the next index within the given found cycle. Adjusts the tailer's state based on the current direction.
     *
     * @param nextCycle The cycle in which to find the next index
     * @return The next available index within the found cycle
     */
    private long nextIndexWithinFoundCycle(final int nextCycle) {
        state = FOUND_IN_CYCLE;
        if (direction == FORWARD)
            return queue.rollCycle().toIndex(nextCycle, 0);

        if (direction == BACKWARD) {
            try {
                long lastSequenceNumber0 = store().lastSequenceNumber(this);
                return queue.rollCycle().toIndex(nextCycle, lastSequenceNumber0);

            } catch (Exception e) {
                throw new AssertionError(e);
            }
        } else {
            throw new IllegalStateException("direction=" + direction);
        }
    }

    /**
     * Retrieves the current index, which includes the cycle number.
     * If an index updater is present, it retrieves the volatile value from it.
     *
     * @return The current index
     */
    @Override
    public long index() {
        return indexUpdater == null ? this.index : indexUpdater.index().getValue();
    }

    /**
     * Retrieves the current cycle number.
     *
     * @return The current cycle number
     */
    @Override
    public int cycle() {
        return this.cycle;
    }

    /**
     * Moves the tailer to the specified index. Depending on the current state and whether the index is near
     * the last move, this method uses various optimization strategies to efficiently reach the target index.
     *
     * @param index The target index to move to
     * @return true if successfully moved to the target index, otherwise false
     */
    @Override
    public boolean moveToIndex(final long index) {
        throwExceptionIfClosed();

        if (moveToState.canReuseLastIndexMove(index, state, direction, queue, privateWire())) {
            return setAddress(true);
        } else if (moveToState.indexIsCloseToAndAheadOfLastIndexMove(index, state, direction, queue)) {
            final long knownIndex = moveToState.lastMovedToIndex;
            final boolean found =
                    this.store.linearScanTo(index, knownIndex, this,
                            moveToState.readPositionAtLastMove) == ScanResult.FOUND;
            if (found) {
                index(index);
                moveToState.onSuccessfulScan(index, direction, privateWire().bytes().readPosition());
            }
            return setAddress(found);
        }

        return moveToIndexInternal(index);
    }

    /**
     * Moves the tailer to the specified cycle.
     *
     * @param cycle The cycle to move to
     * @return true if successfully moved to the target cycle, otherwise false
     */
    @Override
    public boolean moveToCycle(final int cycle) {
        throwExceptionIfClosed();

        moveToState.indexMoveCount++;
        final ScanResult scanResult = moveToCycleResult0(cycle);
        return scanResult == FOUND;
    }

    /**
     * Sets the wire's position if it is not null and returns whether the index was found.
     *
     * @param found Indicates whether the target index was found.
     * @return true if the wire is set and index was found, otherwise false.
     */
    private boolean setAddress(final boolean found) {
        final Wire wire = privateWire();
        if (wire == null) {
            return false;
        }
        return found;
    }

    /**
     * Attempts to move the tailer to the specified cycle and sets the index accordingly.
     * If the cycle is invalid, the method will return {@link ScanResult#NOT_REACHED}.
     *
     * @param cycle The cycle to move to.
     * @return A {@link ScanResult} indicating whether the cycle was reached or not.
     */
    private ScanResult moveToCycleResult0(final int cycle) {
        if (cycle < 0)
            return NOT_REACHED;
        final RollCycle rollCycle = queue.rollCycle();
//        if (Jvm.isResourceTracing()) {
//            Jvm.debug().on(getClass(), "moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
//        }

        // moves to the expected cycle
        if (!cycle(cycle))
            return NOT_REACHED;

        long index = rollCycle.toIndex(cycle, 0);
        index(index);
        this.store().moveToStartForRead(this);
        final Bytes<?> bytes = privateWire().bytes();

        state = FOUND_IN_CYCLE;
        moveToState.onSuccessfulLookup(index, direction, bytes.readPosition());

        return FOUND;
    }

    /**
     * Attempts to move the tailer to the specified index. If the index is invalid, the method will return {@link ScanResult#NOT_REACHED}.
     *
     * @param index The index to move to.
     * @return A {@link ScanResult} indicating whether the index was reached or not.
     */
    private ScanResult moveToIndexResult0(final long index) {
        if (index < 0)
            return NOT_REACHED;
        final RollCycle rollCycle = queue.rollCycle();
        final int cycle = rollCycle.toCycle(index);
        final long sequenceNumber = rollCycle.toSequenceNumber(index);
//        if (Jvm.isResourceTracing()) {
//            Jvm.debug().on(getClass(), "moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
//        }

        // moves to the expected cycle
        if (!cycle(cycle))
            return NOT_REACHED;

        index(index);
        SingleChronicleQueueStore store0 = this.store();
        if (store0 == null)
            return NOT_REACHED;
        final ScanResult scanResult = store0.moveToIndexForRead(this, sequenceNumber);
        switch (scanResult) {
            case FOUND:
                Wire privateWire = privateWire();
                if (privateWire == null) {
                    state = END_OF_CYCLE;
                } else {
                    state = FOUND_IN_CYCLE;
                    moveToState.onSuccessfulLookup(index, direction, privateWire.bytes().readPosition());
                }
                break;

            case NOT_REACHED:
                state = NOT_REACHED_IN_CYCLE;
                break;
            case NOT_FOUND:
                if (this.cycle < this.queue.lastCycle()) {
                    state = END_OF_CYCLE;
                    return END_OF_FILE;
                }
                break;
            case END_OF_FILE:
                state = END_OF_CYCLE;
                break;
            default:
                throw new IllegalStateException("Unknown ScanResult: " + scanResult);
        }

        return scanResult;
    }

    /**
     * Moves the tailer to the specified index.
     *
     * @param index The index to move to.
     * @return The {@link ScanResult} indicating whether the move was successful or not.
     */
    ScanResult moveToIndexResult(final long index) {
        final ScanResult scanResult = moveToIndexResult0(index);
        return scanResult;
    }

    /**
     * Moves the tailer to the start of the queue, resetting the position and state.
     *
     * @return The current tailer instance.
     */
    private ExcerptTailer doToStart() {
        assert direction != BACKWARD;
        final int firstCycle = queue.firstCycle();
        if (firstCycle == Integer.MAX_VALUE) {
            state = UNINITIALISED;
            return this;
        }
        if (firstCycle != this.cycle) {
            // Move to the first cycle if it differs
            final boolean found = cycle(firstCycle);
            if (found)
                state = FOUND_IN_CYCLE;
            else if (store != null)
                throw new MissingStoreFileException("Missing first store file cycle=" + firstCycle);
        }
        index(queue.rollCycle().toIndex(cycle, 0));

        state = FOUND_IN_CYCLE;
        Wire wire = privateWire();
        if (wire != null) {
            wire.bytes().readPosition(0);
        }
        return this;
    }

    /**
     * Moves the tailer to the start of the queue. Throws an exception if a document is being read when the method is called.
     *
     * @return The current tailer instance.
     */
    @NotNull
    @Override
    public final ExcerptTailer toStart() {
        if (context.isPresent())
            throw new IllegalStateException("Cannot move tailer to start during document reading");

        try {
            return doToStart();
        } catch (MissingStoreFileException e) {
            queue.refreshDirectoryListing();
            return doToStart();
        }
    }

    /**
     * Internally moves the tailer to the specified index.
     *
     * @param index The index to move to.
     * @return true if the move was successful, false otherwise.
     */
    private boolean moveToIndexInternal(final long index) {
        moveToState.indexMoveCount++;
        final ScanResult scanResult = moveToIndexResult0(index);
        return scanResult == FOUND;
    }

    /**
     * gives approximately the last index, can not be relied on as the last index may have changed just after this was called. For this reason, this
     * code is not in queue as it should only be an internal method
     *
     * @return the last index at the time this method was called, or Long.MIN_VALUE if none.
     */
    private long approximateLastIndex() {

        final int lastCycle = queue.lastCycle();
        try {
            if (lastCycle == Integer.MIN_VALUE)
                return Long.MIN_VALUE;

            return approximateLastCycle2(lastCycle);

        } catch (@NotNull StreamCorruptedException | UnrecoverableTimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Approximates the last cycle for the specified cycle, and attempts to retrieve the last sequence number within that cycle.
     * If the sequence number is -1, it tries to find the last cycle that contains data.
     *
     * @param lastCycle The last cycle to approximate the last entry for.
     * @return The index of the last entry in the specified cycle, or the index of the last entry in the queue.
     * @throws StreamCorruptedException  if there is a corruption in the data stream.
     * @throws MissingStoreFileException if the store file for the specified cycle is missing.
     */
    private long approximateLastCycle2(int lastCycle) throws StreamCorruptedException, MissingStoreFileException {
        final RollCycle rollCycle = queue.rollCycle();
        final SingleChronicleQueueStore wireStore = (cycle == lastCycle) ? this.store : queue.storeForCycle(
                lastCycle, queue.epoch(), false, this.store);

        long sequenceNumber = -1;
        if (wireStore != null) {
            if (this.store != wireStore) {
                releaseStore();
                this.store = wireStore;
                this.cycle = lastCycle;
                resetWires();
            }

            sequenceNumber = this.store().lastSequenceNumber(this);
        }
        // give the position of the last entry and
        // flag we want to count it even though we don't know if it will be meta data or not.

        // fixes #378
        if (sequenceNumber == -1L) {
            // nothing has been written yet, so point to start of cycle
            int firstCycle = queue.firstCycle();
            while (firstCycle < lastCycle) {
                lastCycle--;
                try {
                    return approximateLastCycle2(lastCycle);
                } catch (MissingStoreFileException e) {
                    // try again.
                }
            }
            this.setCycle(firstCycle);
            return rollCycle.toIndex(firstCycle, 0L);
        }

        this.setCycle(lastCycle);
        return rollCycle.toIndex(lastCycle, sequenceNumber);
    }

    /**
     * Performs a header number check by comparing the expected header number with the actual header number.
     * If the actual header number does not match the expected value, a warning is logged.
     *
     * @param wire The wire to perform the header check on.
     * @return true if the header check passes, false otherwise.
     */
    @SuppressWarnings("java:S3516")
    private boolean headerNumberCheck(@NotNull final Wire wire) {
        if (!(wire instanceof AbstractWire)) {
            return true;
        }

        ((AbstractWire) wire).headNumberCheck((actual, position) -> {
            try {
                final long expecting = store.sequenceForPosition(this, position, false);
                if (actual == expecting)
                    return true;
                Jvm.warn().on(getClass(), new AssertionError("header number check failed " +
                        "expecting=" + expecting +
                        "  !=  actual=" + actual));

                return false;
            } catch (Exception e) {
                Jvm.warn().on(getClass(), "", e);
                return false;
            }
        });

        return true;
    }

    /**
     * Resets the wire for both the main context and index, ensuring the wires are properly aligned with the store.
     * This is called when switching to a different cycle or when refreshing the wire state.
     */
    private void resetWires() {
        final WireType wireType = queue.wireType();

        SingleChronicleQueueStore s = store;
        if (s == null) return;

        final MappedBytes bytes = s.bytes();
        bytes.singleThreadedCheckDisabled(singleThreadedCheckDisabled());

        final Wire wire2 = wireType.apply(bytes);
        wire2.usePadding(s.dataVersion() > 0);
        final Wire wire = readAnywhere(wire2);
        assert !QueueSystemProperties.CHECK_INDEX || headerNumberCheck(wire);
        this.context.wire(wire);
        wire.parent(this);

        final Wire wireForIndexOld = wireForIndex;
        wireForIndex = readAnywhere(wireType.apply(s.bytes()));
        assert !QueueSystemProperties.CHECK_INDEX || headerNumberCheck(wireForIndex);
        assert wire != wireForIndexOld;

        if (wireForIndexOld != null) {
            wireForIndexOld.bytes().releaseLast();
        }
    }

    /**
     * Reads the wire, ensuring it can be read from any position within the file.
     * This method sets the wire's read limit to its capacity.
     *
     * @param wire The wire to read from.
     * @return The wire instance after applying padding and resetting the read position.
     */
    @NotNull
    private Wire readAnywhere(@NotNull final Wire wire) {
        final Bytes<?> bytes = wire.bytes();
        bytes.readLimitToCapacity();

        SingleChronicleQueueStore s = store;
        if (s != null)
            wire.usePadding(s.dataVersion() > 0);

        return wire;
    }

    /**
     * Moves the tailer to the end of the queue. If the tailer is reading in a backward direction, it calls the original implementation.
     * If not, it attempts to move to the end using an optimized approach.
     *
     * @return The tailer instance after being moved to the end.
     * @throws IllegalStateException if the tailer is currently reading a document.
     */
    @NotNull
    @Override
    public ExcerptTailer toEnd() {
        throwExceptionIfClosed();

        if (context.isPresent())
            throw new IllegalStateException("Cannot move tailer to end during document reading");

        if (direction.equals(TailerDirection.BACKWARD)) {
            return callOriginalToEnd();
        }

        return callOptimizedToEnd();
    }

    /**
     * Calls an optimized method to move the tailer to the end of the queue.
     * If a store file is missing, it refreshes the directory listing and retries using the original method.
     *
     * @return The tailer instance after being moved to the end.
     */
    private ExcerptTailer callOptimizedToEnd() {
        try {
            return optimizedToEnd();
        } catch (MissingStoreFileException e) {
            queue.refreshDirectoryListing();
            return originalToEnd();
        }
    }

    /**
     * Moves the tailer to the end of the Chronicle Queue, attempting the original end operation first.
     * If a {@link NotReachedException} occurs due to a race condition, it retries after refreshing the directory listing.
     *
     * @return The tailer positioned at the end of the queue or at the start if the end could not be reached.
     */
    @NotNull
    private ExcerptTailer callOriginalToEnd() {
        try {
            return originalToEnd();
        } catch (NotReachedException e) {
            queue.refreshDirectoryListing();
            // due to a race condition, where the queue rolls as we are processing toEnd()
            // we may get a NotReachedException  ( see https://github.com/OpenHFT/Chronicle-Queue/issues/702 )
            // hence are just going to retry.
            try {
                return originalToEnd();
            } catch (Exception ex) {
                Jvm.warn().on(getClass(), "Unable to find toEnd() so winding to the start " + ex);
                return toStart();
            }
        }
    }

    /**
     * Enables or disables striding mode on the tailer.
     *
     * @param striding True to enable striding mode, false to disable.
     * @return The current tailer instance with the updated striding setting.
     */
    @Override
    public ExcerptTailer striding(final boolean striding) {
        throwExceptionIfClosedInSetter();

        this.striding = striding;
        return this;
    }

    /**
     * Indicates whether striding mode is enabled for the tailer.
     *
     * @return True if striding is enabled, false otherwise.
     */
    @Override
    public boolean striding() {
        return striding;
    }

    /**
     * Optimized method to move the tailer to the end of the queue, reducing overhead for faster performance.
     *
     * @return The tailer positioned at the end of the queue.
     */
    @NotNull
    private ExcerptTailer optimizedToEnd() {
        final RollCycle rollCycle = queue.rollCycle();
        final int lastCycle = queue.lastCycle();
        try {
            if (lastCycle == Integer.MIN_VALUE) {
                if (state() == TailerState.CYCLE_NOT_FOUND)
                    state = UNINITIALISED;
                return this;
            }

            final SingleChronicleQueueStore wireStore = queue.storeForCycle(
                    lastCycle, queue.epoch(), false, this.store);
            if (wireStore == null)
                throw new MissingStoreFileException("Store not found for cycle " + Long.toHexString(lastCycle) + ". Probably the files were removed? queue=" + queue.fileAbsolutePath());
            this.setCycle(lastCycle);

            if (this.store != wireStore) {
                releaseStore();
                this.store = wireStore;
                resetWires();
            }
            // give the position of the last entry and
            // flag we want to count it even though we don't know if it will be meta data or not.

            Wire w = privateWire();

            if (w == null) {
                return callOriginalToEnd();
            }

            final long sequenceNumber = wireStore.moveToEndForRead(w);

            // fixes #378
            if (sequenceNumber == -1L) {
                return callOriginalToEnd();
            }

            final Bytes<?> bytes = w.bytes();
            state = isEndOfFile(bytes.readVolatileInt(bytes.readPosition())) ? END_OF_CYCLE : FOUND_IN_CYCLE;

            index(rollCycle.toIndex(wireStore.cycle(), sequenceNumber));

        } catch (@NotNull UnrecoverableTimeoutException e) {
            throw new IllegalStateException(e);
        }

        return this;
    }

    /**
     * Moves the tailer to the original end of the queue, checking each index.
     *
     * @return The tailer positioned at the end of the queue.
     */
    @NotNull
    public ExcerptTailer originalToEnd() {
        throwExceptionIfClosed();

        long approximateLastIndex = approximateLastIndex();
        long index = approximateLastIndex;

        if (index == Long.MIN_VALUE) {
            if (state() == TailerState.CYCLE_NOT_FOUND)
                state = UNINITIALISED;
            return this;
        }
        ScanResult scanResult = moveToIndexResult(index);
        switch (scanResult) {
            case NOT_FOUND:
                if (moveToIndexResult(index - 1) == FOUND)
                    state = FOUND_IN_CYCLE;
                break;

            case FOUND:
                LoopForward: // NOSONAR
                while (originalToEndLoopCondition(approximateLastIndex, index)) {
                    final ScanResult result = moveToIndexResult(++index);
                    switch (result) {
                        case NOT_REACHED:
                            throw new NotReachedException("NOT_REACHED after FOUND");
                        case FOUND:
                            // the end moved!!
                            continue;
                        case NOT_FOUND:
                            state = FOUND_IN_CYCLE;
                            break LoopForward;
                        case END_OF_FILE:
                            state = END_OF_CYCLE;
                            break LoopForward;
                        default:
                            throw new IllegalStateException("Unknown ScanResult: " + result);
                    }
                }

                break;
            case NOT_REACHED:
                throw new NotReachedException("NOT_REACHED index: " + Long.toHexString(index));
            case END_OF_FILE:
                state = END_OF_CYCLE;
                break;
            default:
                throw new IllegalStateException("Unknown ScanResult: " + scanResult);
        }

        return this;

    }

    /**
     * Loop condition to check if the original end logic should continue.
     *
     * @param approximateLastIndex The approximate last index in the queue.
     * @param index                The current index being checked.
     * @return True if the loop should continue, false otherwise.
     */
    private boolean originalToEndLoopCondition(long approximateLastIndex, long index) {
        if (direction == FORWARD) {
            return true;
        } else if (direction == BACKWARD) {
            // Do not let index run past the approximate last index
            return index < approximateLastIndex;
        } else {
            return true;
        }
    }

    /**
     * Retrieves the current direction of the tailer.
     *
     * @return The current {@link TailerDirection} of the tailer.
     */
    @Override
    public TailerDirection direction() {
        return direction;
    }

    /**
     * Sets the direction of the tailer and moves to the current index if switching from BACKWARD to FORWARD.
     *
     * @param direction The new {@link TailerDirection} for the tailer.
     * @return The current tailer instance with the updated direction.
     */
    @NotNull
    @Override
    public ExcerptTailer direction(@NotNull final TailerDirection direction) {
        throwExceptionIfClosedInSetter();

        final TailerDirection oldDirection = this.direction();
        this.direction = direction;
        if (oldDirection == TailerDirection.BACKWARD &&
                direction == TailerDirection.FORWARD) {
            moveToIndexInternal(index());
        }

        return this;
    }

    /**
     * Returns the associated {@link ChronicleQueue} instance.
     *
     * @return The ChronicleQueue that this tailer is reading from.
     */
    @Override
    @NotNull
    public ChronicleQueue queue() {
        return queue;
    }

    /**
     * Increments the index based on the current {@link TailerDirection}.
     * If moving forward and the sequence number overflows, it rolls over to the next cycle.
     * If moving backward and the sequence number is negative, it winds back to the previous cycle.
     */
    @PackageLocal
    void incrementIndex() {
        final RollCycle rollCycle = queue.rollCycle();
        final long index = this.index();

        // Handle the case where no index is set and the direction is forward
        if (index == -1 && direction == FORWARD) {
            index0(0);
            return;
        }
        long seq = rollCycle.toSequenceNumber(index);
        final int cycle = rollCycle.toCycle(index);

        seq += direction.add();
        switch (direction) {
            case NONE:
                break;
            case FORWARD:
                // if it runs out of seq number it will flow over to tomorrows cycle file
                if (rollCycle.toSequenceNumber(seq) < seq) {
                    if (this.cycle != cycle + 1) {
                        cycle(cycle + 1);
                        Jvm.warn().on(getClass(),
                                "we have run out of sequence numbers, so will start to write to " +
                                        "the next .cq4 file, the new cycle=" + cycle);
                    }
                    seq = 0;
                }
                break;
            case BACKWARD:
                // Wind back if the sequence number is negative
                if (seq < 0) {
                    windBackCycle(cycle);
                    return;
                } else if (seq > 0 && striding) {
                    // Adjust sequence number to stride boundary if striding is enabled
                    seq -= seq % rollCycle.defaultIndexSpacing();
                }
                break;
        }
        index0(rollCycle.toIndex(cycle, seq));

    }

    /**
     * Moves the tailer back to the previous cycle if possible.
     *
     * @param cycle The current cycle number.
     */
    private void windBackCycle(int cycle) {
        final long first = queue.firstCycle();
        while (--cycle >= first)
            if (tryWindBack(cycle))
                return;

        this.index(queue.rollCycle().toIndex(cycle, -1));
        this.state = BEYOND_START_OF_CYCLE;
    }

    /**
     * Attempts to move back to a previous cycle and update the index accordingly.
     *
     * @param cycle The cycle to attempt winding back to.
     * @return True if the cycle was successfully moved to, false otherwise.
     */
    private boolean tryWindBack(final int cycle) {
        final long count = excerptsInCycle(cycle);
        if (count <= 0)
            return false;
        final RollCycle rollCycle = queue.rollCycle();
        if (moveToIndexInternal(rollCycle.toIndex(cycle, count - 1))) {
            this.state = FOUND_IN_CYCLE;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Updates the internal index to the specified value.
     *
     * @param index The new index value to set.
     */
    void index0(final long index) {
        if (indexUpdater == null) {
            this.index = index;
        } else {
            indexUpdater.update(index);
        }
    }

    // DON'T INLINE THIS METHOD, as it's used by enterprise chronicle queue
    void index(final long index) {
        index0(index);

        if (indexAtCreation == Long.MIN_VALUE) {
            indexAtCreation = index;
        }

        moveToState.reset();
    }

    /**
     * Moves the tailer to the specified cycle, resetting wires and state if necessary.
     *
     * @param cycle The cycle number to move to.
     * @return True if the cycle was successfully moved to, false otherwise.
     */
    private boolean cycle(final int cycle) {
        if (this.cycle == cycle && (state == FOUND_IN_CYCLE || state == NOT_REACHED_IN_CYCLE))
            return true;

        final SingleChronicleQueueStore nextStore = queue.storeForCycle(
                cycle, queue.epoch(), false, this.store);

        if (nextStore == null && store == null)
            return false;

        if (nextStore == null) {
            if (direction == BACKWARD)
                state = BEYOND_START_OF_CYCLE;
            else
                state = CYCLE_NOT_FOUND;
            return false;
        }

        if (nextStore == store) {
            return true;
        }

        releaseStore();

        context.wire(null);
        store = nextStore;
        state = FOUND_IN_CYCLE;
        setCycle(cycle);
        resetWires();
        final Wire wire = privateWire();
        wire.parent(this);
        wire.pauser(queue.pauserSupplier.get());
        return true;
    }

    /**
     * Releases the current store and resets the state to UNINITIALISED.
     */
    void releaseStore() {
        if (store != null) {
            storePool.closeStore(store);
            store = null;
        }
        state = UNINITIALISED;
    }

    /**
     * Enables or disables reading after replica acknowledgment.
     *
     * @param readAfterReplicaAcknowledged True to enable reading after acknowledgment, false otherwise.
     */
    @Override
    public void readAfterReplicaAcknowledged(final boolean readAfterReplicaAcknowledged) {
        throwExceptionIfClosed();
        this.readAfterReplicaAcknowledged = readAfterReplicaAcknowledged;
    }

    /**
     * Sets the {@link AcknowledgedIndexReplicatedCheck} to be used for determining if an index has been replicated.
     *
     * @param acknowledgedIndexReplicatedCheck The check implementation for replicated indexes.
     */
    @Override
    public void acknowledgedIndexReplicatedCheck(final @NotNull AcknowledgedIndexReplicatedCheck acknowledgedIndexReplicatedCheck) {
        readAfterReplicaAcknowledged(true);
        this.acknowledgedIndexReplicatedCheck = acknowledgedIndexReplicatedCheck;
    }

    /**
     * Checks if the tailer is reading after replica acknowledgment.
     *
     * @return True if reading after replica acknowledgment is enabled, false otherwise.
     */
    @Override
    public boolean readAfterReplicaAcknowledged() {
        throwExceptionIfClosed();

        return readAfterReplicaAcknowledged;
    }

    /**
     * Returns the number of excerpts (messages) in the given cycle.
     *
     * @param cycle The cycle to check.
     * @return The number of excerpts in the cycle, or -1 if the cycle could not be found.
     */
    @Override
    public long excerptsInCycle(int cycle) {
        throwExceptionIfClosed();
        try {
            return moveToCycle(cycle) ? store.lastSequenceNumber(this) + 1 : -1;
        } catch (StreamCorruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            releaseStore();
        }
    }

    /**
     * Returns the current state of the tailer.
     *
     * @return The current {@link TailerState}.
     */
    @NotNull
    @Override
    public TailerState state() {
        return state;
    }

    public void setCycle(final int cycle) {
        this.cycle = cycle;
    }

    // visible for testing
    int getIndexMoveCount() {
        return moveToState.indexMoveCount;
    }

    /**
     * Returns the current store, initializing it if necessary.
     *
     * @return The current {@link SingleChronicleQueueStore}.
     */
    @NotNull
    private SingleChronicleQueueStore store() {
        if (store == null)
            if (!cycle(cycle()))
                Jvm.warn().on(getClass(), "Unable to find cycle=" + cycle() + ", queue=" + queue.fileAbsolutePath());
        return store;
    }

    /**
     * Returns the file corresponding to the current cycle in the store.
     *
     * @return The current file or null if no store is available.
     */
    @Override
    public File currentFile() {
        SingleChronicleQueueStore store = this.store;
        return store == null ? null : store.currentFile();
    }

    /**
     * Synchronizes the tailer's underlying store to disk. This ensures that all data up to the
     * current read position is flushed to disk, providing durability for the queue.
     * If the store is null, the method returns without performing any operation.
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void sync() {
        if (store == null)
            return;

        final Bytes<?> bytes = privateWire().bytes();
        BytesStore store = bytes.bytesStore();

        // If the byte store is memory-mapped, synchronize it
        if (store instanceof MappedBytesStore) {
            MappedBytesStore mbs = (MappedBytesStore) store;
            mbs.syncUpTo(bytes.readPosition());
            queue.lastIndexMSynced(lastReadIndex);
        }
    }

    /**
     * Helper class that tracks the state of index movement within the tailer.
     * This is used to optimize subsequent index moves by reusing state information.
     */
    static final class MoveToState {
        private long lastMovedToIndex = Long.MIN_VALUE;
        private TailerDirection directionAtLastMoveTo = TailerDirection.NONE;
        private long readPositionAtLastMove = Long.MIN_VALUE;
        private int indexMoveCount = 0;

        /**
         * Updates the state after successfully looking up an index.
         *
         * @param movedToIndex The index moved to.
         * @param direction    The direction of the movement.
         * @param readPosition The read position at the new index.
         */
        void onSuccessfulLookup(final long movedToIndex,
                                final TailerDirection direction,
                                final long readPosition) {
            this.lastMovedToIndex = movedToIndex;
            this.directionAtLastMoveTo = direction;
            this.readPositionAtLastMove = readPosition;
        }

        /**
         * Updates the state after successfully scanning to an index.
         *
         * @param movedToIndex The index moved to.
         * @param direction    The direction of the movement.
         * @param readPosition The read position at the new index.
         */
        void onSuccessfulScan(final long movedToIndex,
                              final TailerDirection direction,
                              final long readPosition) {
            this.lastMovedToIndex = movedToIndex;
            this.directionAtLastMoveTo = direction;
            this.readPositionAtLastMove = readPosition;
        }

        /**
         * Resets the state, clearing all stored values.
         */
        void reset() {
            lastMovedToIndex = Long.MIN_VALUE;
            directionAtLastMoveTo = TailerDirection.NONE;
            readPositionAtLastMove = Long.MIN_VALUE;
        }

        /**
         * Determines if the last index move can be reused based on proximity to the target index
         * and if the state and direction match.
         *
         * @param index     The target index.
         * @param state     The current tailer state.
         * @param direction The direction of movement.
         * @param queue     The ChronicleQueue instance.
         * @return True if the last move can be reused, false otherwise.
         */
        private boolean indexIsCloseToAndAheadOfLastIndexMove(final long index,
                                                              final TailerState state,
                                                              final TailerDirection direction,
                                                              final ChronicleQueue queue) {
            return lastMovedToIndex != Long.MIN_VALUE &&
                    index - lastMovedToIndex < INDEXING_LINEAR_SCAN_THRESHOLD &&
                    state == FOUND_IN_CYCLE &&
                    direction == directionAtLastMoveTo &&
                    queue.rollCycle().toCycle(index) == queue.rollCycle().toCycle(lastMovedToIndex) &&
                    index > lastMovedToIndex;
        }

        /**
         * Checks if the last index move can be reused based on the current state and index.
         *
         * @param index     The target index.
         * @param state     The current tailer state.
         * @param direction The direction of movement.
         * @param queue     The ChronicleQueue instance.
         * @param wire      The current wire.
         * @return True if the last index move can be reused, false otherwise.
         */
        private boolean canReuseLastIndexMove(final long index,
                                              final TailerState state,
                                              final TailerDirection direction,
                                              final ChronicleQueue queue,
                                              final Wire wire) {

            return ((wire == null) || wire.bytes().readPosition() == readPositionAtLastMove) &&
                    index == this.lastMovedToIndex && index != 0 && state == FOUND_IN_CYCLE &&
                    direction == directionAtLastMoveTo &&
                    queue.rollCycle().toCycle(index) == queue.rollCycle().toCycle(lastMovedToIndex);
        }
    }

    /**
     * Finalizer class to warn and close resources if the StoreTailer has not been closed properly.
     * This helps in detecting resource leaks by generating warnings when the object is garbage collected.
     */
    private class Finalizer {
        @SuppressWarnings({"deprecation", "removal"})
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            warnAndCloseIfNotClosed();
        }
    }

    /**
     * Context class used by the StoreTailer to manage reading documents and tracking state.
     * Extends BinaryReadDocumentContext and adds methods for managing wire and metadata.
     */
    class StoreTailerContext extends BinaryReadDocumentContext {
        StoreTailerContext() {
            super(null);
        }

        /**
         * Returns the index being processed by the tailer.
         *
         * @return The index value.
         */
        @Override
        public long index() {
            return StoreTailer.this.index();
        }

        /**
         * Returns the source ID of the tailer.
         *
         * @return The source ID.
         */
        @Override
        public int sourceId() {
            return StoreTailer.this.sourceId();
        }

        /**
         * Closes the context, and if necessary, increments the index after reading a document.
         */
        @Override
        public void close() {
            if (rollbackIfNeeded())
                return;

            if (isPresent() && !isMetaData())
                incrementIndex();

            super.close();
        }

        /**
         * Updates the presence flag for the current document.
         *
         * @param present Whether the document is present.
         * @return The updated presence flag.
         */
        boolean present(final boolean present) {
            return this.present = present;
        }

        /**
         * Sets the wire for the context, releasing the old wire if necessary.
         *
         * @param wire The new wire to set.
         */
        public void wire(@Nullable final Wire wire) {
            if (wire == this.wire)
                return;

            final Wire oldWire = this.wire;
            this.wire = wire;

            if (oldWire != null)
                oldWire.bytes().release(INIT); // might be held elsewhere if used for another purpose.
        }

        /**
         * Marks whether the current document is metadata or not.
         *
         * @param metaData True if the document is metadata, false otherwise.
         */
        public void metaData(boolean metaData) {
            this.metaData = metaData;
        }

        @Override
        public String toString() {
            return wire == null ? "unset" : super.toString();
        }
    }
}
