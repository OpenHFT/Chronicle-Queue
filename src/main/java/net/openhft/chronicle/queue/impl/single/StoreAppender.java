/*
 * Copyright 2016-2022 chronicle.software
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

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueSystemProperties;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.queue.impl.WireStoreSupplier;
import net.openhft.chronicle.queue.impl.table.AbstractTSQueueLock;
import net.openhft.chronicle.queue.util.*;
import net.openhft.chronicle.wire.*;
import net.openhft.chronicle.wire.domestic.InternalWire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.BufferOverflowException;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.WARN_SLOW_APPENDER_MS;
import static net.openhft.chronicle.wire.Wires.*;

/**
 * This class represents an appender for a single chronicle queue, allowing for appending
 * excerpts to the queue. It manages the cycle of the queue, lock handling, and the state
 * of the wire and store.
 */
class StoreAppender extends AbstractCloseable
        implements ExcerptAppender, ExcerptContext, InternalAppender, MicroTouched {

    /**
     * Key for storing the normalised EOF information in the table-store to avoid re-normalizing older cycles.
     */
    private static final String NORMALISED_EOFS_TO_TABLESTORE_KEY = "normalisedEOFsTo";
    @NotNull
    private final SingleChronicleQueue queue; // The associated queue
    @NotNull
    private final WriteLock writeLock; // The lock for writes
    private final WriteLock appendLock; // Lock for appends

    @NotNull
    private final StoreAppenderContext context; // The context used for appending
    private final WireStorePool storePool; // Pool for managing wire stores
    private final boolean checkInterrupts; // Flag to check for interrupts during append
    @UsedViaReflection
    private final Finalizer finalizer; // Used for cleanup if resource tracing is enabled
    @Nullable
    SingleChronicleQueueStore store; // Current store in use
    long lastPosition; // The last known position
    private int cycle = Integer.MIN_VALUE; // The current cycle
    @Nullable
    private Wire wire; // The wire for reading/writing data
    @Nullable
    private Wire wireForIndex; // Wire used for indexing operations
    private long positionOfHeader = 0; // Position of the current header
    private long lastIndex = Long.MIN_VALUE; // The last index written
    @Nullable
    private Pretoucher pretoucher = null; // Optional pretoucher for optimizing IO
    private MicroToucher microtoucher = null; // Optional microtoucher for small data optimization
    private Wire bufferWire = null; // Buffer wire used for double-buffered writes
    private int count = 0; // Internal counter for tracking appends

    /**
     * Constructor for StoreAppender. Initializes the appender by finding the first open cycle
     * and setting up the appropriate resources for writing.
     *
     * @param queue           The chronicle queue to append to
     * @param storePool       The pool for managing wire stores
     * @param checkInterrupts Flag to indicate whether to check for interrupts during operations
     */
    StoreAppender(@NotNull final SingleChronicleQueue queue,
                  @NotNull final WireStorePool storePool,
                  final boolean checkInterrupts) {
        this.queue = queue;
        this.storePool = storePool;
        this.checkInterrupts = checkInterrupts;
        this.writeLock = queue.writeLock();
        this.appendLock = queue.appendLock();
        this.context = new StoreAppenderContext();
        this.finalizer = Jvm.isResourceTracing() ? new Finalizer() : null;

        try {
            int lastExistingCycle = queue.lastCycle(); // Get the last cycle in the queue
            int firstCycle = queue.firstCycle(); // Get the first cycle in the queue
            long start = System.nanoTime(); // Start time for performance tracking
            final WriteLock writeLock = this.queue.writeLock();
            writeLock.lock();
            try {
                // Process cycles and handle EOF markers
                if (firstCycle != Integer.MAX_VALUE) {
                    // Backing down until EOF-ed cycle is encountered
                    for (int eofCycle = lastExistingCycle; eofCycle >= firstCycle; eofCycle--) {
                        setCycle2(eofCycle, WireStoreSupplier.CreateStrategy.READ_ONLY);
                        if (cycleHasEOF()) {
                            // Normalize EOFs for older cycles
                            if (eofCycle > firstCycle)
                                normaliseEOFs0(eofCycle - 1);

                            // If a non-EOF file exists, ensure future cycles are set up
                            if (eofCycle < lastExistingCycle)
                                setCycle2(eofCycle + 1 /* TODO: Position on existing one? */, WireStoreSupplier.CreateStrategy.READ_ONLY);
                            break;
                        }
                    }
                    if (wire != null)
                        resetPosition(); // Reset position after processing cycles
                }
            } finally {
                writeLock.unlock();
                long tookMillis = (System.nanoTime() - start) / 1_000_000; // Measure time taken
                if (tookMillis > WARN_SLOW_APPENDER_MS || (lastExistingCycle >= 0 && cycle != lastExistingCycle))
                    Jvm.perf().on(getClass(), "Took " + tookMillis + "ms to find first open cycle " + cycle);
            }
        } catch (RuntimeException ex) {
            // If an exception occurs during initialization, close the appender and rethrow the exception
            close();

            throw ex;
        }

        // Add the appender as a close listener for the queue
        queue.addCloseListener(this);
    }

    /**
     * Checks if the current cycle has an end-of-file (EOF) marker.
     *
     * @return true if the cycle has an EOF marker, false otherwise
     */
    private boolean cycleHasEOF() {
        if (wire != null) {
            assert this.queue.writeLock().locked(); // Ensure the write lock is held
            assert this.store != null; // Ensure the store is not null

            // Try reserving bytes for this operation
            if (wire.bytes().tryReserve(this)) {
                try {
                    // Check if the wire contains an EOF marker
                    return WireOut.EndOfWire.PRESENT == wire.endOfWire(false, timeoutMS(), TimeUnit.MILLISECONDS, store.writePosition());
                } finally {
                    wire.bytes().release(this); // Release the reserved bytes
                }
            }
        }

        return false;
    }

    /**
     * Releases the resources associated with the given wire, if any.
     *
     * @param w the wire whose resources are to be released
     */
    private static void releaseBytesFor(Wire w) {
        if (w != null) {
            w.bytes().release(INIT); // Release the bytes associated with the wire
        }
    }

    /**
     * Checks the append lock to determine if appending is allowed. This version
     * assumes that the process holding the lock is not the current process.
     */
    private void checkAppendLock() {
        checkAppendLock(false); // Default behavior is to not allow the current process to bypass the lock
    }

    /**
     * Checks the append lock, with an option to allow the current process to bypass the lock.
     *
     * @param allowMyProcess If true, allows the current process to bypass the append lock.
     */
    private void checkAppendLock(boolean allowMyProcess) {
        if (appendLock.locked())
            checkAppendLockLocked(allowMyProcess);
    }

    /**
     * Verifies if the append lock is held by another process and throws an exception if appending is not allowed.
     * This method is called when the lock is held.
     *
     * @param allowMyProcess If true, the current process is allowed to append even if the lock is held.
     */
    private void checkAppendLockLocked(boolean allowMyProcess) {
        // Perform lock check only when the append lock is an instance of AbstractTSQueueLock
        if (appendLock instanceof AbstractTSQueueLock) {
            final AbstractTSQueueLock appendLock = (AbstractTSQueueLock) this.appendLock;
            final long lockedBy = appendLock.lockedBy(); // Identify which process holds the lock

            if (lockedBy == AbstractTSQueueLock.UNLOCKED)
                return; // No lock is held

            boolean myPID = lockedBy == Jvm.getProcessId(); // Check if the lock is held by this process
            if (allowMyProcess && myPID)
                return; // Allow appending if the current process is allowed

            throw new IllegalStateException("locked: unable to append because a lock is being held by pid=" + (myPID ? "me" : lockedBy) + ", file=" + queue.file());
        } else
            throw new IllegalStateException("locked: unable to append, file=" + queue.file());
    }

    /**
     * Writes a marshallable object to the excerpt.
     *
     * @param marshallable The object to write into the excerpt.
     */
    @Override
    public void writeBytes(@NotNull final WriteBytesMarshallable marshallable) {
        throwExceptionIfClosed(); // Ensure the appender is not closed

        try (DocumentContext dc = writingDocument()) { // Start writing a new document
            Bytes<?> bytes = dc.wire().bytes();
            long wp = bytes.writePosition(); // Save the current write position
            marshallable.writeMarshallable(bytes); // Write the marshallable content

            // Rollback if no data was written
            if (wp == bytes.writePosition())
                dc.rollbackOnClose();
        }
    }

    /**
     * Handles the cleanup when the appender is closed, releasing resources and closing the store.
     */
    @Override
    protected void performClose() {
        releaseBytesFor(wireForIndex); // Release wire for indexing
        releaseBytesFor(wire); // Release primary wire
        releaseBytesFor(bufferWire); // Release buffer wire used for double-buffering

        if (pretoucher != null)
            pretoucher.close(); // Close the pretoucher if it exists

        if (store != null) {
            storePool.closeStore(store); // Close the store through the pool
            store = null;
        }

        storePool.close(); // Close the store pool

        // Reset resources to null
        pretoucher = null;
        wireForIndex = null;
        wire = null;
        bufferWire = null;
    }

    /**
     * Ensures that the pretouch operation is performed on the same thread as the appender.
     * If needed, creates a pretoucher to optimize disk IO.
     */
    @Override
    @SuppressWarnings("deprecation")
    public void pretouch() {
        throwExceptionIfClosed(); // Ensure the appender is not closed

        try {
            if (pretoucher == null)
                pretoucher = queue.createPretoucher(); // Create a pretoucher if it doesn't exist

            pretoucher.execute(); // Execute the pretouch operation
        } catch (Throwable e) {
            Jvm.warn().on(getClass(), e);
            throw Jvm.rethrow(e);
        }
    }

    /**
     * Executes a micro-touch, which may optimize small data access for this appender.
     *
     * @return true if the micro-touch operation is successful, false otherwise.
     */
    @Override
    public boolean microTouch() {
        throwExceptionIfClosed(); // Ensure the appender is not closed

        if (microtoucher == null)
            microtoucher = new MicroToucher(this); // Create a new MicroToucher if it doesn't exist

        return microtoucher.execute(); // Execute the micro-touch operation
    }

    /**
     * Performs a background micro-touch operation on this appender.
     * Throws an exception if the appender is already closed.
     */
    @Override
    public void bgMicroTouch() {
        if (isClosed())
            throw new ClosedIllegalStateException(getClass().getName() + " closed for " + Thread.currentThread().getName(), closedHere);

        if (microtoucher == null)
            microtoucher = new MicroToucher(this); // Create a new MicroToucher if it doesn't exist

        microtoucher.bgExecute(); // Execute the background micro-touch
    }

    /**
     * @return the wire associated with this appender.
     */
    @Nullable
    @Override
    public Wire wire() {
        return wire;
    }

    /**
     * @return the wire used for indexing in this appender.
     */
    @Nullable
    @Override
    public Wire wireForIndex() {
        return wireForIndex;
    }

    /**
     * @return the timeout in milliseconds for operations in this appender.
     */
    @Override
    public long timeoutMS() {
        return queue.timeoutMS;
    }

    /**
     * Sets the last index written by this appender.
     *
     * @param index The last index to be set.
     */
    void lastIndex(long index) {
        this.lastIndex = index;
    }

    /**
     * @return true if the appender should record history, false otherwise.
     */
    @Override
    public boolean recordHistory() {
        return sourceId() != 0;
    }

    /**
     * Sets the cycle for this appender.
     *
     * @param cycle The cycle to be set.
     */
    void setCycle(int cycle) {
        if (cycle != this.cycle)
            setCycle2(cycle, WireStoreSupplier.CreateStrategy.CREATE); // Set a new cycle with creation strategy
    }

    /**
     * Sets the cycle for this appender, managing the wire and store transitions if needed.
     * It acquires a new store for the specified cycle and resets the wire positions accordingly.
     *
     * @param cycle          The cycle to set for the appender.
     * @param createStrategy The strategy used to create a new store.
     */
    private void setCycle2(final int cycle, final WireStoreSupplier.CreateStrategy createStrategy) {
        queue.throwExceptionIfClosed(); // Ensure the queue is not closed
        if (cycle < 0)
            throw new IllegalArgumentException("You can not have a cycle that starts " +
                    "before Epoch. cycle=" + cycle); // Ensure the cycle is valid

        SingleChronicleQueue queue = this.queue;

        SingleChronicleQueueStore oldStore = this.store;

        // Acquire a new store for the specified cycle
        SingleChronicleQueueStore newStore = storePool.acquire(cycle, createStrategy, oldStore);

        // If the store has changed, update and close the old one
        if (newStore != oldStore) {
            this.store = newStore;
            if (oldStore != null)
                storePool.closeStore(oldStore);
        }

        // Reset the wires after changing the store
        resetWires(queue);

        // Set the cycle after the wire has been initialized
        this.cycle = cycle;

        if (this.store == null)
            return;

        wire.parent(this);
        wire.pauser(queue.pauserSupplier.get()); // Set the pauser for the wire
        resetPosition(); // Reset the wire position
        queue.onRoll(cycle); // Notify the queue of the cycle change
    }

    /**
     * Resets the wires (primary and indexing) for this appender based on the store.
     * Releases any existing wire resources before creating new ones.
     *
     * @param queue The ChronicleQueue instance to reset wires for.
     */
    private void resetWires(@NotNull final ChronicleQueue queue) {
        WireType wireType = queue.wireType(); // Get the wire type for the queue
        {
            Wire oldw = this.wire;
            this.wire = store == null ? null : createWire(wireType); // Create a new wire
            assert wire != oldw || wire == null;
            releaseBytesFor(oldw); // Release the old wire's resources
        }
        {
            Wire old = this.wireForIndex;
            this.wireForIndex = store == null ? null : createWire(wireType); // Create a new wire for indexing
            assert wireForIndex != old || wireForIndex == null;
            releaseBytesFor(old); // Release the old wire's resources
        }
    }

    /**
     * Creates a new wire for the appender based on the wire type and store bytes.
     * Sets the padding based on the data version.
     *
     * @param wireType The wire type used to create the wire.
     * @return The created Wire object.
     */
    private Wire createWire(@NotNull final WireType wireType) {
        final Wire w = wireType.apply(store.bytes()); // Create a wire based on the store bytes
        w.usePadding(store.dataVersion() > 0); // Use padding if data version > 0
        return w;
    }

    /**
     * Resets the position of the wire to the last write position and updates the header number.
     * Verifies that the position and header number are valid and consistent.
     *
     * @return true if the header number changed, otherwise false.
     * @throws UnrecoverableTimeoutException If a timeout occurs during the operation.
     */
    private boolean resetPosition() {
        long originalHeaderNumber = wire.headerNumber(); // Store the original header number
        try {
            if (store == null || wire == null)
                return false;

            long position = store.writePosition(); // Get the write position from the store
            position(position, position); // Set the position in the wire

            Bytes<?> bytes = wire.bytes();
            assert !QueueSystemProperties.CHECK_INDEX || checkPositionOfHeader(bytes); // Check if the header position is valid

            final long lastSequenceNumber = store.lastSequenceNumber(this); // Get the last sequence number from the store
            wire.headerNumber(queue.rollCycle().toIndex(cycle, lastSequenceNumber + 1) - 1); // Update the header number

            assert !QueueSystemProperties.CHECK_INDEX || wire.headerNumber() != -1 || checkIndex(wire.headerNumber(), positionOfHeader);

            bytes.writeLimit(bytes.capacity()); // Set the write limit to the capacity of the bytes

            assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();
            return originalHeaderNumber != wire.headerNumber(); // Return whether the header number changed

        } catch (@NotNull BufferOverflowException | StreamCorruptedException e) {
            throw new AssertionError(e); // Handle buffer overflow or corruption errors
        }
    }

    /**
     * Checks the validity of the header position in the wire's bytes.
     *
     * @param bytes The Bytes object representing the wire's data.
     * @return true if the header position is valid, otherwise false.
     */
    private boolean checkPositionOfHeader(final Bytes<?> bytes) {
        if (positionOfHeader == 0) {
            return true; // If the position of the header is not set, it's valid by default
        }
        int header = bytes.readVolatileInt(positionOfHeader); // Read the header at the position
        // Check if the header is ready or incomplete
        return isReadyData(header) || isReadyMetaData(header) || isNotComplete(header);
    }

    @NotNull
    @Override
    // throws UnrecoverableTimeoutException
    public DocumentContext writingDocument() {
        return writingDocument(false); // avoid overhead of a default method.
    }


    /**
     * Prepares and returns the write context for writing a document.
     *
     * @param metaData Whether the document contains metadata.
     * @return The prepared DocumentContext for writing.
     * @throws UnrecoverableTimeoutException If a timeout occurs while preparing the context.
     */
    @NotNull
    @Override
    // throws UnrecoverableTimeoutException
    public DocumentContext writingDocument(final boolean metaData) {
        throwExceptionIfClosed(); // Ensure the appender is not closed
        checkAppendLock(metaData); // Check the append lock before writing
        count++; // Increment the write count
        try {
            return prepareAndReturnWriteContext(metaData); // Prepare and return the write context
        } catch (RuntimeException e) {
            count--; // Decrement the count if an exception occurs
            throw e; // Rethrow the exception
        }
    }

    /**
     * Prepares and returns the {@link StoreAppenderContext} for writing data. This method checks if
     * the context needs to be reopened, locks the writeLock, handles double buffering if enabled,
     * and ensures the wire and cycle are set correctly for appending.
     *
     * @param metaData indicates if the write context is for metadata
     * @return the prepared {@link StoreAppenderContext} ready for writing
     */
    private StoreAppender.StoreAppenderContext prepareAndReturnWriteContext(boolean metaData) {
        if (count > 1) {
            assert metaData == context.metaData;
            return context;
        }

        // Handle double buffering if applicable
        if (queue.doubleBuffer && writeLock.locked() && !metaData) {
            prepareDoubleBuffer();
        } else {
            writeLock.lock(); // Acquire the write lock

            try {
                int cycle = queue.cycle();
                if (wire == null)
                    setWireIfNull(cycle); // Set the wire for the current cycle if null

                if (this.cycle != cycle)
                    rollCycleTo(cycle); // Roll over to a new cycle if needed

                long safeLength = queue.overlapSize();
                resetPosition(); // Reset the position to the correct point in the wire
                assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();

                // sets the writeLimit based on the safeLength
                openContext(metaData, safeLength);

                // Move readPosition to the start of the context. i.e. readRemaining() == 0
                wire.bytes().readPosition(wire.bytes().writePosition());
            } catch (RuntimeException e) {
                writeLock.unlock();
                throw e; // Ensure lock is released even if an error occurs
            }
        }

        return context;
    }

    /**
     * Prepares the buffer for double buffering during writes. This involves allocating an elastic
     * buffer and associating it with the current wire type for temporary writing.
     */
    private void prepareDoubleBuffer() {
        context.isClosed = false;
        context.rollbackOnClose = false;
        context.buffered = true;
        if (bufferWire == null) {
            Bytes<?> bufferBytes = Bytes.allocateElasticOnHeap(); // Allocate buffer memory
            bufferWire = queue().wireType().apply(bufferBytes); // Apply the wire type to the buffer
        }
        context.wire = bufferWire; // Set the wire to the buffer wire
        context.metaData(false);
    }

    /**
     * Acquires a document for writing, ensuring the context is prepared. If a document is already open
     * in the context, it reuses the same context unless it's a new chain element.
     *
     * @param metaData indicates if the document is for metadata
     * @return the current {@link DocumentContext} for writing
     */
    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) {
        if (!DISABLE_SINGLE_THREADED_CHECK)
            this.threadSafetyCheck(true);
        if (context.wire != null && context.isOpen() && context.chainedElement())
            return context; // Reuse the existing open context
        return writingDocument(metaData); // Otherwise, open a new document for writing
    }

    /**
     * Ensures that EOF markers are properly added to all cycles, normalizing older cycles to ensure they are complete.
     * This method locks the writeLock and calls the internal {@link #normaliseEOFs0(int)} method for each cycle.
     */
    public void normaliseEOFs() {
        long start = System.nanoTime(); // Track the time taken for normalization
        final WriteLock writeLock = queue.writeLock();
        writeLock.lock();
        try {
            normaliseEOFs0(cycle); // Perform EOF normalization for the current cycle
        } finally {
            writeLock.unlock();
            long tookMillis = (System.nanoTime() - start) / 1_000_000;
            if (tookMillis > WARN_SLOW_APPENDER_MS)
                Jvm.perf().on(getClass(), "Took " + tookMillis + "ms to normaliseEOFs");
        }
    }

    /**
     * Internal method to normalize EOFs for all cycles up to the specified cycle.
     * Adds EOF markers where necessary and ensures all earlier cycles are finalized.
     *
     * @param cycle the target cycle up to which EOF normalization should occur
     */
    private void normaliseEOFs0(int cycle) {
        int first = queue.firstCycle();

        if (first == Integer.MAX_VALUE)
            return;

        final LongValue normalisedEOFsTo = queue.tableStoreAcquire(NORMALISED_EOFS_TO_TABLESTORE_KEY, first);
        int eofCycle = Math.max(first, (int) normalisedEOFsTo.getVolatileValue());
        if (Jvm.isDebugEnabled(StoreAppender.class)) {
            Jvm.debug().on(StoreAppender.class, "Normalising from cycle " + eofCycle);
        }

        // Process each cycle and add EOF markers
        for (; eofCycle < Math.min(queue.cycle(), cycle); ++eofCycle) {
            setCycle2(eofCycle, WireStoreSupplier.CreateStrategy.REINITIALIZE_EXISTING);
            if (wire != null) {
                assert queue.writeLock().locked();
                store.writeEOF(wire, timeoutMS());
                normalisedEOFsTo.setMaxValue(eofCycle); // Update the normalised EOF value
            }
        }
    }

    /**
     * Ensures the wire is set for the specified cycle, normalizing EOFs as needed.
     * If no wire exists, it creates a new wire for the current cycle.
     *
     * @param cycle the cycle for which the wire should be set
     */
    private void setWireIfNull(final int cycle) {
        normaliseEOFs0(cycle); // Normalize EOFs for earlier cycles
        setCycle2(cycle, WireStoreSupplier.CreateStrategy.CREATE); // Set the wire for the specified cycle
    }

    /**
     * Writes a header for the current wire, ensuring the correct position and header number
     * is set for the next write operation.
     *
     * @param wire       the {@link Wire} to write the header to
     * @param safeLength the safe length of data that can be written
     * @return the position of the written header
     */
    private long writeHeader(@NotNull final Wire wire, final long safeLength) {
        Bytes<?> bytes = wire.bytes();
        // writePosition points at the last record in the queue, so we can just skip it and we're ready for write
        long pos = positionOfHeader;
        long lastPos = store.writePosition();
        if (pos < lastPos) {
            // Recalculate header number if the queue has moved since the last operation
            try {
                wire.headerNumber(queue.rollCycle().toIndex(cycle, store.lastSequenceNumber(this)));
            } catch (StreamCorruptedException ex) {
                Jvm.warn().on(getClass(), "Couldn't find last sequence", ex);
            }
        }
        int header = bytes.readVolatileInt(lastPos);
        assert header != NOT_INITIALIZED;
        lastPos += lengthOf(bytes.readVolatileInt(lastPos)) + SPB_HEADER_SIZE;
        bytes.writePosition(lastPos);

        return wire.enterHeader(safeLength); // Write the header and return the new position
    }

    /**
     * Opens a new write context for appending data, setting up the necessary parameters such as
     * the header, write position, and metadata flag.
     *
     * @param metaData  indicates if the context is for metadata
     * @param safeLength the maximum length of data that can be safely written
     */
    private void openContext(final boolean metaData, final long safeLength) {
        assert wire != null;
        this.positionOfHeader = writeHeader(wire, safeLength); // Set header and write position
        context.isClosed = false; // Mark context as open
        context.rollbackOnClose = false; // Disable rollback on close
        context.buffered = false; // Mark context as unbuffered
        context.wire = wire; // Use the current wire for this context
        context.metaData(metaData); // Set metadata flag
    }

    /**
     * Checks if the current header number matches the expected sequence in the queue.
     * Throws an {@link AssertionError} if there is a mismatch.
     *
     * @return true if the header number is valid, false otherwise
     */
    boolean checkWritePositionHeaderNumber() {
        if (wire == null || wire.headerNumber() == Long.MIN_VALUE) return true;

        try {
            long pos = positionOfHeader;

            long seq1 = queue.rollCycle().toSequenceNumber(wire.headerNumber() + 1) - 1;
            long seq2 = store.sequenceForPosition(this, pos, true);

            if (seq1 != seq2) {
                // Generate an error message when the sequence numbers don't match
                String message = "~~~~~~~~~~~~~~ " +
                        "thread: " + Thread.currentThread().getName() +
                        " pos: " + pos +
                        " header: " + wire.headerNumber() +
                        " seq1: " + seq1 +
                        " seq2: " + seq2;
                throw new AssertionError(message);
            }
        } catch (Exception e) {
            // TODO FIX
            Jvm.warn().on(getClass(), e); // Log a warning and rethrow the exception
            throw Jvm.rethrow(e);
        }
        return true;
    }

    /**
     * Returns the source ID of this appender's queue.
     *
     * @return the source ID
     */
    @Override
    public int sourceId() {
        return queue.sourceId;
    }

    /**
     * Writes the provided {@link BytesStore} to the queue. Locks the queue before writing
     * and ensures the wire and cycle are correctly set for the operation.
     *
     * @param bytes the {@link BytesStore} containing the data to be written
     */
    @Override
    public void writeBytes(@NotNull final BytesStore<?, ?> bytes) {
        throwExceptionIfClosed(); // Check if the appender is closed
        checkAppendLock(); // Ensure append lock is acquired
        writeLock.lock(); // Lock for writing

        try {
            int cycle = queue.cycle();
            if (wire == null)
                setWireIfNull(cycle); // Set wire if it doesn't exist

            if (this.cycle != cycle)
                rollCycleTo(cycle); // Roll to the next cycle if needed

            this.positionOfHeader = writeHeader(wire, (int) queue.overlapSize()); // Write the header

            assert isInsideHeader(wire); // Ensure we are inside the correct header
            beforeAppend(wire, wire.headerNumber() + 1); // Prepare for appending
            Bytes<?> wireBytes = wire.bytes();
            wireBytes.write(bytes); // Write the provided bytes to the wire
            wire.updateHeader(positionOfHeader, false, 0); // Update the header
            lastIndex(wire.headerNumber()); // Store the last written index
            lastPosition = positionOfHeader;
            store.writePosition(positionOfHeader); // Update the store's write position
            writeIndexForPosition(lastIndex, positionOfHeader); // Write index for the position
        } catch (StreamCorruptedException e) {
            throw new AssertionError(e); // Handle corruption
        } finally {
            writeLock.unlock(); // Release the write lock
        }
    }

    /**
     * Checks if the current wire is inside a valid header. For certain wire types, this method
     * will validate if the current position is within a header.
     *
     * @param wire the {@link Wire} to check
     * @return true if inside a valid header, false otherwise
     */
    private boolean isInsideHeader(Wire wire) {
        return (wire instanceof AbstractWire) ? ((AbstractWire) wire).isInsideHeader() : true;
    }

    /**
     * Writes the provided {@link BytesStore} to the queue at the specified index.
     * Acquires a write lock before performing the operation.
     *
     * @param index the index to write at
     * @param bytes the data to be written
     */
    @Override
    public void writeBytes(final long index, @NotNull final BytesStore<?, ?> bytes) {
        throwExceptionIfClosed(); // Check if the appender is closed
        checkAppendLock(); // Ensure append lock is acquired
        writeLock.lock(); // Lock for writing

        try {
            writeBytesInternal(index, bytes); // Write bytes internally
        } finally {
            writeLock.unlock(); // Release the write lock
        }
    }

    /**
     * Internal method to write bytes without acquiring the write lock. This method assumes
     * the write lock is acquired externally. Never use this method without locking as it
     * can corrupt the queue file and cause data loss.
     *
     * @param index the index to append at
     * @param bytes the bytes to append
     * @throws IndexOutOfBoundsException if the specified index is not after the end of the queue
     */
    protected void writeBytesInternal(final long index, @NotNull final BytesStore<?, ?> bytes) {
        checkAppendLock(true); // Check append lock for internal use

        final int cycle = queue.rollCycle().toCycle(index);

        if (wire == null)
            setWireIfNull(cycle); // Set wire if null

        /// if the header number has changed then we will have roll
        if (this.cycle != cycle)
            rollCycleTo(cycle, this.cycle > cycle); // Roll cycle if necessary

        // in case our cached headerNumber is incorrect.
        resetPosition(); // Reset wire position if the header number changed

        long headerNumber = wire.headerNumber();

        boolean isNextIndex = index == headerNumber + 1;
        if (!isNextIndex) {
            if (index > headerNumber + 1)
                throw new IllegalIndexException(index, headerNumber);

            // this can happen when using queue replication when we are back filling from a number of sinks at them same time
            // its normal behaviour in the is use case so should not be a WARN
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), "Trying to overwrite index " + Long.toHexString(index) + " which is before the end of the queue");
            return;
        }

        writeBytesInternal(bytes, false);
        //assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();

        headerNumber = wire.headerNumber();
        boolean isIndex = index == headerNumber;
        if (!isIndex) {
            throw new IllegalStateException("index: " + index + ", header: " + headerNumber);
        }
    }

    /**
     * Internal method for writing bytes to the queue without acquiring the write lock. Assumes
     * the write lock is already held by the caller. This method opens the context and writes
     * the provided {@link BytesStore} to the current wire.
     *
     * @param bytes    the data to be written
     * @param metadata flag indicating if the data is metadata
     */
    private void writeBytesInternal(@NotNull final BytesStore<?, ?> bytes, boolean metadata) {
        assert writeLock.locked(); // Ensure the write lock is held

        try {
            int safeLength = (int) queue.overlapSize(); // Calculate safe length for writing
            assert count == 0 : "count=" + count; // Ensure the count is zero before writing
            openContext(metadata, safeLength); // Open the context for writing

            try {
                final Bytes<?> bytes0 = context.wire().bytes(); // Get the bytes from the wire
                bytes0.readPosition(bytes0.writePosition()); // Move the read position to the write position
                bytes0.write(bytes); // Write the provided bytes to the wire
            } finally {
                context.close(false); // Close the context after writing
                count = 0; // Reset the count
            }
        } finally {
            context.isClosed = true; // Ensure the context is marked as closed
        }
    }

    /**
     * Sets the position and validates that the position is within the correct range for the
     * queue's write position. If the position exceeds the allowable range, an exception is thrown.
     *
     * @param position        the new position to set
     * @param startOfMessage  the start of the message within the queue
     */
    private void position(final long position, final long startOfMessage) {
        // Ensure the position doesn't jump too far ahead
        if (position > store.writePosition() + queue.blockSize())
            throw new IllegalArgumentException("pos: " + position + ", store.writePosition()=" +
                    store.writePosition() + " queue.blockSize()=" + queue.blockSize());
        position0(position, startOfMessage, wire.bytes()); // Set the position
    }

    /**
     * Returns the index of the last appended entry. If no entries have been appended,
     * it throws an exception indicating that no data has been appended yet.
     *
     * @return the last appended index
     */
    @Override
    public long lastIndexAppended() {
        if (lastIndex != Long.MIN_VALUE)
            return lastIndex;

        if (lastPosition == Long.MIN_VALUE || wire == null) {
            throw new IllegalStateException("nothing has been appended, so there is no last index");
        }

        try {
            long sequenceNumber = store.sequenceForPosition(this, lastPosition, true);
            long index = queue.rollCycle().toIndex(cycle, sequenceNumber);
            lastIndex(index); // Update the last index
            return index;
        } catch (Exception e) {
            throw Jvm.rethrow(e); // Rethrow exceptions that occur
        }
    }

    /**
     * Returns the current cycle of the queue. If the cycle has not been set, it will determine the
     * cycle based on the last cycle or the current cycle of the queue.
     *
     * @return the current cycle
     */
    @Override
    public int cycle() {
        if (cycle == Integer.MIN_VALUE) {
            int cycle = this.queue.lastCycle(); // Get the last cycle from the queue
            if (cycle < 0)
                cycle = queue.cycle(); // If no last cycle, use the current cycle
            return cycle;
        }
        return cycle; // Return the current cycle
    }

    /**
     * Returns the associated {@link SingleChronicleQueue} for this appender.
     *
     * @return the queue associated with this appender
     */
    @Override
    @NotNull
    public SingleChronicleQueue queue() {
        return queue;

    }

    /**
     * Method to handle operations before appending data to the wire. This method can be overridden
     * in subclasses such as delta wire implementations to add additional behavior before an append.
     *
     * @param wire  the wire to append data to
     * @param index the index of the data to append
     */
    @SuppressWarnings("unused")
    void beforeAppend(final Wire wire, final long index) {
        // Overridden in subclasses, e.g., delta wire
    }

    /**
     * Rolls the current cycle of the queue to the specified cycle, performing necessary
     * operations such as writing EOF markers and switching the current wire and store.
     *
     * @param toCycle the target cycle to roll to
     */
    // throws UnrecoverableTimeoutException

    private void rollCycleTo(final int toCycle) {
        rollCycleTo(toCycle, this.cycle > toCycle); // Roll the cycle with suppression of EOF if required
    }

    /**
     * Rolls the current cycle to the specified target cycle. If the cycle is being rolled
     * forward, it writes EOF markers to the current wire before rolling.
     *
     * @param cycle       the target cycle to roll to
     * @param suppressEOF flag to suppress writing EOF markers
     */
    private void rollCycleTo(final int cycle, boolean suppressEOF) {
        // If the cycle is the same, something went wrong
        if (this.cycle == cycle)
            throw new AssertionError();

        if (!suppressEOF) {
            assert queue.writeLock().locked(); // Ensure write lock is held
            store.writeEOF(wire, timeoutMS()); // Write EOF marker to the wire
        }

        int lastExistingCycle = queue.lastCycle(); // Get the last cycle

        // If we're behind the target cycle, roll forward to the last existing cycle first
        if (lastExistingCycle < cycle && lastExistingCycle != this.cycle && lastExistingCycle >= 0) {
            setCycle2(lastExistingCycle, WireStoreSupplier.CreateStrategy.READ_ONLY); // Set the cycle to the last existing cycle
            rollCycleTo(cycle); // Continue rolling to the target cycle
        } else {
            setCycle2(cycle, WireStoreSupplier.CreateStrategy.CREATE); // Set cycle to the target cycle
        }
    }

    /**
     * Writes the index for a given position in the queue. This method updates the sequence number
     * for the given index and associates it with the provided position.
     *
     * @param index    the index to write
     * @param position the position associated with the index
     * @throws StreamCorruptedException if the index is corrupted
     */
    void writeIndexForPosition(final long index, final long position) throws StreamCorruptedException {
        long sequenceNumber = queue.rollCycle().toSequenceNumber(index); // Convert index to sequence number
        store.setPositionForSequenceNumber(this, sequenceNumber, position); // Set position for the sequence number
    }

    /**
     * Verifies that the index matches the expected sequence number for the given position.
     * Throws an assertion error if the index is incorrect or if a discrepancy is found.
     *
     * @param index    the index to check
     * @param position the position associated with the index
     * @return true if the index is correct, false otherwise
     */
    boolean checkIndex(final long index, final long position) {
        try {
            // Get the expected sequence numbers
            final long seq1 = queue.rollCycle().toSequenceNumber(index + 1) - 1;
            final long seq2 = store.sequenceForPosition(this, position, true);

            // If the sequence numbers don't match, log an error and perform a linear scan
            if (seq1 != seq2) {
                final long seq3 = store.indexing
                        .linearScanByPosition(wireForIndex(), position, 0, 0, true);
                Jvm.error().on(getClass(),
                        "Thread=" + Thread.currentThread().getName() +
                                " pos: " + position +
                                " seq1: " + Long.toHexString(seq1) +
                                " seq2: " + Long.toHexString(seq2) +
                                " seq3: " + Long.toHexString(seq3));

//                System.out.println(store.dump());

                assert seq1 == seq3 : "seq1=" + seq1 + ", seq3=" + seq3;
                assert seq1 == seq2 : "seq1=" + seq1 + ", seq2=" + seq2;

            }
        } catch (@NotNull EOFException | UnrecoverableTimeoutException | StreamCorruptedException e) {
            throw new AssertionError(e);
        }
        return true;
    }

    /**
     * Returns a string representation of the current state of the StoreAppender,
     * including information about the queue, cycle, position, last index, and last position.
     *
     * @return a string representation of the StoreAppender
     */
    @Override
    public String toString() {
        return "StoreAppender{" +
                "queue=" + queue +
                ", cycle=" + cycle +
                ", position=" + positionOfHeader +
                ", lastIndex=" + lastIndex +
                ", lastPosition=" + lastPosition +
                '}';
    }

    /**
     * Sets the internal position and adjusts the {@link Bytes} instance to ensure the write limit
     * and position are properly set. This method is used to manage the position of data within the
     * queue.
     *
     * @param position        the position to set
     * @param startOfMessage  the starting position of the message in the bytes
     * @param bytes           the {@link Bytes} instance associated with the current wire
     */
    void position0(final long position, final long startOfMessage, Bytes<?> bytes) {
        this.positionOfHeader = position; // Set the header position
        bytes.writeLimit(bytes.capacity()); // Set the write limit to the capacity
        bytes.writePosition(startOfMessage); // Set the write position to the start of the message
    }

    /**
     * Returns the current file associated with this appender. If no store is available,
     * returns null.
     *
     * @return the current file or null if no store is available
     */
    @Override
    public File currentFile() {
        SingleChronicleQueueStore store = this.store;
        return store == null ? null : store.currentFile();
    }

    /**
     * Synchronizes the data to disk by ensuring that any data written to memory is persisted. This
     * method is typically used for {@link MappedBytesStore} instances. If no store or wire is
     * available, this method does nothing.
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void sync() {
        if (store == null || wire == null)
            return;

        final Bytes<?> bytes = wire.bytes(); // Get the current wire bytes
        BytesStore store = bytes.bytesStore();
        if (store instanceof MappedBytesStore) {
            MappedBytesStore mbs = (MappedBytesStore) store;
            mbs.syncUpTo(bytes.writePosition()); // Sync the data up to the current write position
            queue.lastIndexMSynced(lastIndex); // Update the last synced index
        }
    }

    /**
     * Indicates whether the writing process is complete. This is determined by the context.
     *
     * @return true if writing is complete, false otherwise
     */
    @Override
    public boolean writingIsComplete() {
        return context.writingIsComplete();
    }

    /**
     * Rolls back the current context if the writing process is not complete.
     */
    @Override
    public void rollbackIfNotComplete() {
        context.rollbackIfNotComplete();
    }

    /**
     * Finalizer for the {@link StoreAppender}. If the appender is not properly closed, it rolls
     * back the context and closes the resources, logging a warning.
     */
    private class Finalizer {
        @SuppressWarnings({"deprecation", "removal"})
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            context.rollbackOnClose(); // Roll back the context if necessary
            warnAndCloseIfNotClosed(); // Warn and close resources if not already closed
        }
    }

    /**
     * The inner class responsible for managing the context of a write operation in the {@link StoreAppender}.
     * This context handles metadata, buffering, and rollback mechanisms for writing operations.
     */
    final class StoreAppenderContext implements WriteDocumentContext {

        boolean isClosed = true; // Flag indicating if the context is closed
        private boolean metaData = false; // Flag indicating if the data is metadata
        private boolean rollbackOnClose = false; // Flag indicating if rollback should occur on close
        private boolean buffered = false; // Flag indicating if the data is buffered
        @Nullable
        private Wire wire; // The wire associated with this context
        private boolean alreadyClosedFound; // Flag to track if the context was already closed
        private StackTrace closedHere; // Stack trace for debugging closures
        private boolean chainedElement; // Flag indicating if the context is part of a chain

        /**
         * Checks if the context is empty by examining the read remaining bytes of the wire.
         *
         * @return true if the context is empty, false otherwise
         */
        public boolean isEmpty() {
            Bytes<?> bytes = wire().bytes(); // Get the bytes from the wire
            return bytes.readRemaining() == 0; // Check if there are no remaining bytes
        }

        /**
         * Resets the context, clearing all flags and state variables.
         */
        @Override
        public void reset() {
            isClosed = true;
            metaData = false;
            rollbackOnClose = false;
            buffered = false;
            alreadyClosedFound = false;
            chainedElement = false;
        }

        /**
         * Returns the source ID associated with the current queue.
         *
         * @return the source ID
         */
        @Override
        public int sourceId() {
            return StoreAppender.this.sourceId();
        }

        /**
         * Indicates whether the context is currently present. This always returns false as
         * this method is intended for metadata-only contexts.
         *
         * @return false always
         */
        @Override
        public boolean isPresent() {
            return false;
        }

        /**
         * Returns the wire associated with this context.
         *
         * @return the wire for this context
         */
        @Override
        public Wire wire() {
            return wire;
        }

        /**
         * Indicates whether the data being written is metadata.
         *
         * @return true if the data is metadata, false otherwise
         */
        @Override
        public boolean isMetaData() {
            return metaData;
        }

        /**
         * Marks the context to roll back when closed if an error condition is detected.
         */
        @Override
        public void rollbackOnClose() {
            this.rollbackOnClose = true; // Set rollback on close
        }

        /**
         * Closes the context, committing or rolling back the changes depending on the state.
         */
        @Override
        public void close() {
            close(true); // Close the context and commit changes by default
        }

        /**
         * Close this {@link StoreAppenderContext}, finalizing the writing process and releasing
         * resources. Depending on the conditions, this method either commits the written data,
         * rolls it back, or clears the buffer.
         *
         * @param unlock true if the {@link StoreAppender#writeLock} should be unlocked.
         */
        public void close(boolean unlock) {
            // Check if closing conditions are met
            if (!closePreconditionsAreSatisfied()) return;

            try {
                handleInterrupts();  // Handle any thread interruptions
                if (handleRollbackOnClose()) return;  // Rollback if necessary

                if (wire == StoreAppender.this.wire) {
                    // Update the header and index if using the same wire
                    updateHeaderAndIndex();
                } else if (wire != null) {
                    // If buffered, write from the buffer and clear it
                    if (buffered) {
                        writeBytes(wire.bytes());
                        unlock = false; // Buffer handled, so no need to unlock
                        wire.clear();
                    } else {
                        // Write bytes normally if no buffering is involved
                        writeBytesInternal(wire.bytes(), metaData);
                        wire = StoreAppender.this.wire; // Reset wire
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();  // Restore interrupt flag
                throw new InterruptedRuntimeException(e);  // Rethrow the exception
            } catch (StreamCorruptedException | UnrecoverableTimeoutException e) {
                throw new IllegalStateException(e);  // Handle potential exceptions
            } finally {
                closeCleanup(unlock);  // Cleanup after closing
            }
        }

        /**
         * Rolls back the context when necessary. Returns true if rollback was performed,
         * otherwise false.
         *
         * @return true if rollback was performed, false otherwise
         */
        private boolean handleRollbackOnClose() {
            if (rollbackOnClose) {
                doRollback();  // Perform rollback if needed
                return true;
            }
            return false;
        }

        /**
         * Ensures that all preconditions for closing the context are satisfied. If not, the method
         * will either skip the closing process or log a warning if the context has already been
         * closed.
         *
         * @return true if preconditions for closing are met, false otherwise
         */
        private boolean closePreconditionsAreSatisfied() {
            if (chainedElement)  // Don't close if part of a chain
                return false;
            if (isClosed) {
                // Log a warning if already closed
                Jvm.warn().on(getClass(), "Already Closed, close was called twice.", new StackTrace("Second close", closedHere));
                alreadyClosedFound = true;
                return false;
            }
            count--;  // Decrement the count, skip closing if still processing
            if (count > 0)
                return false;

            if (alreadyClosedFound) {
                closedHere = new StackTrace("Closed here");  // Capture stack trace if already closed
            }
            return true;
        }

        /**
         * Handles potential interruptions in the current thread during the closing process.
         * If interrupts are detected and handling is enabled, the method will throw an
         * {@link InterruptedException}.
         *
         * @throws InterruptedException if the thread is interrupted
         */
        private void handleInterrupts() throws InterruptedException {
            final boolean interrupted = checkInterrupts && Thread.currentThread().isInterrupted();
            if (interrupted)
                throw new InterruptedException();
        }

        /**
         * Updates the header and index after writing. Ensures that the correct position is stored
         * and, if needed, notifies listeners about the appending process.
         *
         * @throws StreamCorruptedException if there is an error updating the header
         */
        private void updateHeaderAndIndex() throws StreamCorruptedException {
            if (wire == null) throw new NullPointerException("Wire must not be null");
            if (store == null) throw new NullPointerException("Store must not be null");

            try {
                // Update the header at the current position
                wire.updateHeader(positionOfHeader, metaData, 0);
            } catch (IllegalStateException e) {
                if (queue.isClosed())  // If the queue is closed, stop further processing
                    return;
                throw e;
            }

            lastPosition = positionOfHeader;  // Store the last known position

            if (!metaData) {
                lastIndex(wire.headerNumber());  // Set the last index
                store.writePosition(positionOfHeader);  // Write the position to the store
                if (lastIndex != Long.MIN_VALUE) {
                    writeIndexForPosition(lastIndex, positionOfHeader);  // Update index with the position
                    if (queue.appenderListener != null) {
                        callAppenderListener();  // Notify listeners if applicable
                    }
                }
            }
        }

        /**
         * Performs cleanup tasks after closing the context. This includes setting the write position
         * and unlocking the {@link StoreAppender#writeLock} if needed.
         *
         * @param unlock true if the {@link StoreAppender#writeLock} should be unlocked.
         */
        private void closeCleanup(boolean unlock) {
            if (wire == null) throw new NullPointerException("Wire must not be null");
            Bytes<?> bytes = wire.bytes();  // Retrieve the bytes associated with the wire
            bytes.writePositionForHeader(true);  // Set the write position
            isClosed = true;  // Mark the context as closed
            if (unlock) {
                try {
                    writeLock.unlock();  // Unlock the write lock if necessary
                } catch (Exception ex) {
                    Jvm.warn().on(getClass(), "Exception while unlocking: ", ex);  // Log any issues during unlocking
                }
            }
        }

        /**
         * Calls the appender listener to process the excerpt at the current position.
         * The read and write positions of the wire are preserved during this operation.
         */
        private void callAppenderListener() {
            final Bytes<?> bytes = wire.bytes();
            long rp = bytes.readPosition();  // Save the current read position
            long wp = bytes.writePosition();  // Save the current write position
            try {
                queue.appenderListener.onExcerpt(wire, lastIndex);  // Invoke the listener with the current excerpt
            } finally {
                // Restore the original read and write positions
                bytes.readPosition(rp);
                bytes.writePosition(wp);
            }
        }

        /**
         * Rolls back the current write operation, clearing any data that was written during the
         * current context. This ensures that no incomplete or erroneous data is committed to the
         * queue.
         */
        private void doRollback() {
            if (buffered) {
                // Clear the buffer if buffering is being used
                assert wire != StoreAppender.this.wire;
                wire.clear();
            } else {
                // Zero out the content written in this context
                final Bytes<?> bytes = wire.bytes();
                try {
                    for (long i = positionOfHeader; i <= bytes.writePosition(); i++)
                        bytes.writeByte(i, (byte) 0);  // Overwrite each byte with zero
                    long lastPosition = StoreAppender.this.lastPosition;
                    position0(lastPosition, lastPosition, bytes);  // Reset the position
                    ((InternalWire)wire).forceNotInsideHeader();  // Ensure the wire is not inside a header
                } catch (BufferOverflowException | IllegalStateException e) {
                    if (bytes instanceof MappedBytes && ((MappedBytes) bytes).isClosed()) {
                        Jvm.warn().on(getClass(), "Unable to roll back excerpt as it is closed.");  // Log a warning if the buffer is closed
                        return;
                    }
                    throw e;
                }
            }
        }

        /**
         * Returns the index of the current context. If the context is using double buffering, an
         * {@link IndexNotAvailableException} will be thrown as the index is not available in this case.
         *
         * @return the index of the current context or {@link Long#MIN_VALUE} if the index is unavailable
         */
        @Override
        public long index() {
            if (buffered) {
                throw new IndexNotAvailableException("Index is unavailable when double buffering");
            }
            if (this.wire == null)
                return Long.MIN_VALUE;
            if (this.wire.headerNumber() == Long.MIN_VALUE) {
                try {
                    // Set the header number based on the last sequence number
                    wire.headerNumber(queue.rollCycle().toIndex(cycle, store.lastSequenceNumber(StoreAppender.this)));
                    long headerNumber0 = wire.headerNumber();
                    assert isInsideHeader(this.wire);
                    return isMetaData() ? headerNumber0 : headerNumber0 + 1;  // Adjust for metadata
                } catch (IOException e) {
                    throw new IORuntimeException(e);
                }
            }

            return isMetaData() ? Long.MIN_VALUE : this.wire.headerNumber() + 1;
        }

        /**
         * @return true if the context is still open and not yet closed
         */
        @Override
        public boolean isOpen() {
            return !isClosed;
        }

        /**
         * @return true if the context has not been fully completed yet
         */
        @Override
        public boolean isNotComplete() {
            return !isClosed;
        }

        /**
         * Unsupported operation in this context.
         *
         * @throws UnsupportedOperationException if this method is called
         */
        @Override
        public void start(boolean metaData) {
            throw new UnsupportedOperationException();
        }

        /**
         * Sets whether the context is for metadata or not.
         *
         * @param metaData true if the context is for metadata, false otherwise
         */
        public void metaData(boolean metaData) {
            this.metaData = metaData;
        }

        /**
         * @return true if the context is part of a chained operation, false otherwise
         */
        @Override
        public boolean chainedElement() {
            return chainedElement;
        }

        /**
         * Sets whether the context is part of a chained operation.
         *
         * @param chainedElement true if the context is part of a chain, false otherwise
         */
        @Override
        public void chainedElement(boolean chainedElement) {
            this.chainedElement = chainedElement;
        }

        /**
         * @return true if the writing process has been completed and the context is closed
         */
        public boolean writingIsComplete() {
            return isClosed;
        }

        /**
         * Rolls back the context if the writing process was not completed. This ensures that no
         * incomplete data is written to the queue.
         */
        @Override
        public void rollbackIfNotComplete() {
            if (isClosed) return;
            chainedElement = false;
            count = 1;
            rollbackOnClose = true;
            close();
        }
    }
}
