/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
import net.openhft.chronicle.queue.util.MicroTouched;
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
     * Keep track of where we've normalised EOFs to, so we don't re-do immutable, older cycles every time.
     * This is the key in the table-store where we store that information
     */
    private static final String NORMALISED_EOFS_TO_TABLESTORE_KEY = "normalisedEOFsTo";
    @NotNull
    private final SingleChronicleQueue queue;
    @NotNull
    private final WriteLock writeLock;
    private final WriteLock appendLock;

    @NotNull
    private final StoreAppenderContext context;
    private final WireStorePool storePool;
    private final boolean checkInterrupts;
    @UsedViaReflection
    private final Finalizer finalizer;
    @Nullable
    SingleChronicleQueueStore store;
    long lastPosition;
    private int cycle = Integer.MIN_VALUE;
    @Nullable
    private Wire wire;
    @Nullable
    private Wire wireForIndex;
    private long positionOfHeader = 0;
    private long lastIndex = Long.MIN_VALUE;
    @Nullable
    private Pretoucher pretoucher = null;
    private MicroToucher microtoucher = null;
    private Wire bufferWire = null;
    private int count = 0;

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
            int lastExistingCycle = queue.lastCycle();
            int firstCycle = queue.firstCycle();
            long start = System.nanoTime();
            final WriteLock writeLock = this.queue.writeLock();
            writeLock.lock();
            try {
                // Process cycles and handle EOF markers
                if (firstCycle != Integer.MAX_VALUE) {
                    // Backing down until EOF-ed cycle is encountered
                    for (int eofCycle = lastExistingCycle; eofCycle >= firstCycle; eofCycle--) {
                        setCycle2(eofCycle, WireStoreSupplier.CreateStrategy.READ_ONLY);
                        if (cycleHasEOF()) {
                            // Make sure all older cycles have EOF marker
                            if (eofCycle > firstCycle)
                                normaliseEOFs0(eofCycle - 1);

                            // If first non-EOF file is in the past, it's possible it will be replicated/backfilled to
                            if (eofCycle < lastExistingCycle)
                                setCycle2(eofCycle + 1 /* TODO: Position on existing one? */, WireStoreSupplier.CreateStrategy.READ_ONLY);
                            break;
                        }
                    }
                    if (wire != null)
                        resetPosition();
                }
            } finally {
                writeLock.unlock();
                long tookMillis = (System.nanoTime() - start) / 1_000_000;
                if (tookMillis > WARN_SLOW_APPENDER_MS || (lastExistingCycle >= 0 && cycle != lastExistingCycle))
                    Jvm.perf().on(getClass(), "Took " + tookMillis + "ms to find first open cycle " + cycle);
            }
        } catch (RuntimeException ex) {
            // Perhaps initialization code needs to be moved away from constructor
            close();

            throw ex;
        }

        // always put references to "this" last.
        queue.addCloseListener(this);
    }

    /**
     * Checks if the current cycle has an end-of-file (EOF) marker.
     *
     * @return true if the cycle has an EOF marker, false otherwise
     */
    private boolean cycleHasEOF() {
        if (wire != null) {
            assert this.queue.writeLock().locked();
            assert this.store != null;

            if (wire.bytes().tryReserve(this)) {
                try {
                    return WireOut.EndOfWire.PRESENT ==
                            wire.endOfWire(false, timeoutMS(), TimeUnit.MILLISECONDS, store.writePosition());
                } finally {
                    wire.bytes().release(this);
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
            w.bytes().release(INIT);
        }
    }

    /**
     * Checks the append lock to determine if appending is allowed. This version
     * assumes that the process holding the lock is not the current process.
     */
    private void checkAppendLock() {
        checkAppendLock(false);
    }

    /**
     * Checks the append lock, with an option to allow the current process to bypass the lock.
     *
     * @param allowMyProcess this will only be true for any writes coming from the sink replicator
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
        // separate method as this is in fast path
        if (appendLock instanceof AbstractTSQueueLock) {
            final AbstractTSQueueLock appendLock = (AbstractTSQueueLock) this.appendLock;
            final long lockedBy = appendLock.lockedBy();
            if (lockedBy == AbstractTSQueueLock.UNLOCKED)
                return;
            boolean myPID = lockedBy == Jvm.getProcessId();
            if (allowMyProcess && myPID)
                return;
            throw new IllegalStateException("locked: unable to append because a lock is being held by pid=" + (myPID ? "me" : lockedBy) + ", file=" + queue.file());
        } else {
            throw new IllegalStateException("locked: unable to append, file=" + queue.file());
        }
    }

    /**
     * Writes a marshallable object to the excerpt.
     *
     * @param marshallable The object to write into the excerpt.
     */
    @Override
    public void writeBytes(@NotNull final WriteBytesMarshallable marshallable) {
        throwExceptionIfClosed();

        try (DocumentContext dc = writingDocument()) {
            Bytes<?> bytes = dc.wire().bytes();
            long wp = bytes.writePosition();
            marshallable.writeMarshallable(bytes);

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
        releaseBytesFor(wireForIndex);
        releaseBytesFor(wire);
        releaseBytesFor(bufferWire);

        if (pretoucher != null)
            pretoucher.close();

        if (store != null) {
            storePool.closeStore(store);
            store = null;
        }

        storePool.close();

        pretoucher = null;
        wireForIndex = null;
        wire = null;
        bufferWire = null;
    }

    /**
     * pretouch() has to be run on the same thread, as the thread that created the appender. If you want to use pretouch() in another thread, you must
     * first create or have an appender that was created on this thread, and then use this appender to call the pretouch()
     */
    @Override
    public void pretouch() {
        throwExceptionIfClosed();

        try {
            if (pretoucher == null)
                pretoucher = queue.createPretoucher();

            pretoucher.execute();

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
        throwExceptionIfClosed();

        if (microtoucher == null)
            microtoucher = new MicroToucher(this);

        return microtoucher.execute();
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
            microtoucher = new MicroToucher(this);

        microtoucher.bgExecute();
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
            setCycle2(cycle, WireStoreSupplier.CreateStrategy.CREATE);
    }

    /**
     * Sets the cycle for this appender, managing the wire and store transitions if needed.
     * It acquires a new store for the specified cycle and resets the wire positions accordingly.
     *
     * @param cycle          The cycle to set for the appender.
     * @param createStrategy The strategy used to create a new store.
     */
    private void setCycle2(final int cycle, final WireStoreSupplier.CreateStrategy createStrategy) {
        queue.throwExceptionIfClosed();
        if (cycle < 0)
            throw new IllegalArgumentException("You can not have a cycle that starts " +
                    "before Epoch. cycle=" + cycle);

        SingleChronicleQueue queue = this.queue;

        SingleChronicleQueueStore oldStore = this.store;

        SingleChronicleQueueStore newStore = storePool.acquire(cycle, createStrategy, oldStore);

        // If the store has changed, update and close the old one
        if (newStore != oldStore) {
            this.store = newStore;
            if (oldStore != null)
                storePool.closeStore(oldStore);
        }
        resetWires(queue);

        // only set the cycle after the wire is set.
        this.cycle = cycle;

        if (this.store == null)
            return;

        wire.parent(this);
        wire.pauser(queue.pauserSupplier.get());
        resetPosition();
        queue.onRoll(cycle);
    }

    /**
     * Resets the wires (primary and indexing) for this appender based on the store.
     * Releases any existing wire resources before creating new ones.
     *
     * @param queue The ChronicleQueue instance to reset wires for.
     */
    private void resetWires(@NotNull final ChronicleQueue queue) {
        WireType wireType = queue.wireType();
        {
            Wire oldw = this.wire;
            this.wire = store == null ? null : createWire(wireType);
            assert wire != oldw || wire == null;
            releaseBytesFor(oldw);
        }
        {
            Wire old = this.wireForIndex;
            this.wireForIndex = store == null ? null : createWire(wireType);
            assert wireForIndex != old || wireForIndex == null;
            releaseBytesFor(old);
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
        final Wire w = wireType.apply(store.bytes());
        w.usePadding(store.dataVersion() > 0);
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
        long originalHeaderNumber = wire.headerNumber();
        long INVALID_HEADER_NUMBER = -1;

        try {
            if (store == null || wire == null)
                return false;
            long position = store.writePosition();
            position(position, position);

            Bytes<?> bytes = wire.bytes();
            assert !QueueSystemProperties.CHECK_INDEX || checkPositionOfHeader(bytes);

            final long lastSequenceNumber = store.lastSequenceNumber(this);
            wire.headerNumber(queue.rollCycle().toIndex(cycle, lastSequenceNumber + 1) - 1);

            assert !QueueSystemProperties.CHECK_INDEX || wire.headerNumber() != INVALID_HEADER_NUMBER ||
                    checkIndex(wire.headerNumber(), positionOfHeader);

            bytes.writeLimit(bytes.capacity());

            assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();
            return originalHeaderNumber != wire.headerNumber();

        } catch (@NotNull BufferOverflowException | StreamCorruptedException e) {
            throw new AssertionError(e);
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
            return true;
        }
        int header = bytes.readVolatileInt(positionOfHeader);
        // ready or an incomplete message header?
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
        throwExceptionIfClosed();
        // we allow the sink process to write metaData
        checkAppendLock(metaData);
        count++;
        try {
            return prepareAndReturnWriteContext(metaData);
        } catch (RuntimeException e) {
            count--;
            throw e;
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

        boolean shouldPrepareDoubleBuffer = queue.doubleBuffer && writeLock.locked() && !metaData;

        if (shouldPrepareDoubleBuffer) {
            prepareDoubleBuffer();
        } else {
            writeLock.lock();

            try {
                int cycle = queue.cycle();
                if (wire == null)
                    setWireIfNull(cycle);

                if (this.cycle != cycle)
                    rollCycleTo(cycle);

                long safeLength = queue.overlapSize();
                resetPosition();
                assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();

                // sets the writeLimit based on the safeLength
                openContext(metaData, safeLength);

                // Move readPosition to the start of the context. i.e. readRemaining() == 0
                wire.bytes().readPosition(wire.bytes().writePosition());
            } catch (RuntimeException e) {
                writeLock.unlock();
                throw e;
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
            Bytes<?> bufferBytes = Bytes.allocateElasticOnHeap();
            bufferWire = queue().wireType().apply(bufferBytes);
        }
        context.wire = bufferWire;
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
            return context;
        return writingDocument(metaData);
    }

    /**
     * Ensures that EOF markers are properly added to all cycles, normalizing older cycles to ensure they are complete.
     * This method locks the writeLock and calls the internal {@link #normaliseEOFs0(int)} method for each cycle.
     */
    public void normaliseEOFs() {
        long start = System.nanoTime();
        final WriteLock writeLock = queue.writeLock();
        writeLock.lock();
        try {
            normaliseEOFs0(cycle);
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

        for (; eofCycle < Math.min(queue.cycle(), cycle); ++eofCycle) {
            setCycle2(eofCycle, WireStoreSupplier.CreateStrategy.REINITIALIZE_EXISTING);
            if (wire != null) {
                assert queue.writeLock().locked();
                store.writeEOF(wire, timeoutMS());
                normalisedEOFsTo.setMaxValue(eofCycle);
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
        normaliseEOFs0(cycle);

        setCycle2(cycle, WireStoreSupplier.CreateStrategy.CREATE);
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
            // queue moved since we last touched it - recalculate header number

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
        return wire.enterHeader(safeLength);
    }

    /**
     * Opens a new write context for appending data, setting up the necessary parameters such as
     * the header, write position, and metadata flag.
     *
     * @param metaData   indicates if the context is for metadata
     * @param safeLength the maximum length of data that can be safely written
     */
    private void openContext(final boolean metaData, final long safeLength) {
        assert wire != null;
        this.positionOfHeader = writeHeader(wire, safeLength); // sets wire.bytes().writePosition = position + 4;
        context.isClosed = false;
        context.rollbackOnClose = false;
        context.buffered = false;
        context.wire = wire; // Jvm.isDebug() ? acquireBufferWire() : wire;
        context.metaData(metaData);
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
            Jvm.warn().on(getClass(), e);
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
        throwExceptionIfClosed();
        checkAppendLock();
        writeLock.lock();
        try {
            int cycle = queue.cycle();
            if (wire == null)
                setWireIfNull(cycle);

            if (this.cycle != cycle)
                rollCycleTo(cycle);

            this.positionOfHeader = writeHeader(wire, (int) queue.overlapSize()); // writeHeader sets wire.byte().writePosition

            assert isInsideHeader(wire);
            beforeAppend(wire, wire.headerNumber() + 1);
            Bytes<?> wireBytes = wire.bytes();
            wireBytes.write(bytes);
            wire.updateHeader(positionOfHeader, false, 0);
            lastIndex(wire.headerNumber());
            lastPosition = positionOfHeader;
            store.writePosition(positionOfHeader);
            writeIndexForPosition(lastIndex, positionOfHeader);
        } catch (StreamCorruptedException e) {
            throw new AssertionError(e);
        } finally {
            writeLock.unlock();
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
        throwExceptionIfClosed();
        checkAppendLock();
        writeLock.lock();
        try {
            writeBytesInternal(index, bytes);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Appends bytes without write lock. Should only be used if write lock is acquired externally. Never use without write locking as it WILL corrupt
     * the queue file and cause data loss.
     *
     * @param index Index to append at
     * @param bytes The excerpt bytes
     * @throws IndexOutOfBoundsException when the index specified is not after the end of the queue
     */
    protected void writeBytesInternal(final long index, @NotNull final BytesStore<?, ?> bytes) {
        checkAppendLock(true);

        final int cycle = queue.rollCycle().toCycle(index);

        if (wire == null)
            setWireIfNull(cycle);

        /// if the header number has changed then we will have roll
        if (this.cycle != cycle)
            rollCycleTo(cycle, this.cycle > cycle);

        // in case our cached headerNumber is incorrect.
        resetPosition();

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

    private void writeBytesInternal(@NotNull final BytesStore<?, ?> bytes, boolean metadata) {
        assert writeLock.locked();
        try {
            int safeLength = (int) queue.overlapSize();
            assert count == 0 : "count=" + count;
            openContext(metadata, safeLength);

            try {
                final Bytes<?> bytes0 = context.wire().bytes();
                bytes0.readPosition(bytes0.writePosition());
                bytes0.write(bytes);
            } finally {
                context.close(false);
                count = 0;
            }
        } finally {
            context.isClosed = true;
        }
    }

    private void position(final long position, final long startOfMessage) {
        // did the position jump too far forward.
        if (position > store.writePosition() + queue.blockSize())
            throw new IllegalArgumentException("pos: " + position + ", store.writePosition()=" +
                    store.writePosition() + " queue.blockSize()=" + queue.blockSize());
        position0(position, startOfMessage, wire.bytes());
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
            lastIndex(index);
            return index;
        } catch (Exception e) {
            throw Jvm.rethrow(e);
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
            int cycle = this.queue.lastCycle();
            if (cycle < 0)
                cycle = queue.cycle();
            return cycle;
        }
        return cycle;
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

    /*
     * overridden in delta wire
     */
    @SuppressWarnings("unused")
    void beforeAppend(final Wire wire, final long index) {
    }

    /**
     * Rolls the current cycle of the queue to the specified cycle, performing necessary
     * operations such as writing EOF markers and switching the current wire and store.
     *
     * @param toCycle the target cycle to roll to
     */
    private void rollCycleTo(final int toCycle) {
        rollCycleTo(toCycle, this.cycle > toCycle);
    }

    /**
     * Rolls the current cycle to the specified target cycle. If the cycle is being rolled
     * forward, it writes EOF markers to the current wire before rolling.
     *
     * @param cycle       the target cycle to roll to
     * @param suppressEOF flag to suppress writing EOF markers
     */
    private void rollCycleTo(final int cycle, boolean suppressEOF) {

        // only a valid check if the wire was set.
        if (this.cycle == cycle)
            throw new AssertionError();

        if (!suppressEOF) {
            assert queue.writeLock().locked();
            store.writeEOF(wire, timeoutMS());
        }

        int lastExistingCycle = queue.lastCycle();

        // If we're behind the target cycle, roll forward to the last existing cycle first
        if (lastExistingCycle < cycle && lastExistingCycle != this.cycle && lastExistingCycle >= 0) {
            setCycle2(lastExistingCycle, WireStoreSupplier.CreateStrategy.READ_ONLY);
            rollCycleTo(cycle);
        } else {
            setCycle2(cycle, WireStoreSupplier.CreateStrategy.CREATE);
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
        long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
        store.setPositionForSequenceNumber(this, sequenceNumber, position);
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
     * @param position       the position to set
     * @param startOfMessage the starting position of the message in the bytes
     * @param bytes          the {@link Bytes} instance associated with the current wire
     */
    void position0(final long position, final long startOfMessage, Bytes<?> bytes) {
        this.positionOfHeader = position;
        bytes.writeLimit(bytes.capacity());
        bytes.writePosition(startOfMessage);
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

        final Bytes<?> bytes = wire.bytes();
        BytesStore store = bytes.bytesStore();
        if (store instanceof MappedBytesStore) {
            MappedBytesStore mbs = (MappedBytesStore) store;
            mbs.syncUpTo(bytes.writePosition());
            queue.lastIndexMSynced(lastIndex);
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
            context.rollbackOnClose();
            warnAndCloseIfNotClosed();
        }
    }

    /**
     * The inner class responsible for managing the context of a write operation in the {@link StoreAppender}.
     * This context handles metadata, buffering, and rollback mechanisms for writing operations.
     */
    final class StoreAppenderContext implements WriteDocumentContext {

        boolean isClosed = true;
        private boolean metaData = false;
        private boolean rollbackOnClose = false;
        private boolean buffered = false;
        @Nullable
        private Wire wire;
        private boolean alreadyClosedFound;
        private StackTrace closedHere;
        private boolean chainedElement;

        /**
         * Checks if the context is empty by examining the read remaining bytes of the wire.
         *
         * @return true if the context is empty, false otherwise
         */
        public boolean isEmpty() {
            Bytes<?> bytes = wire().bytes();
            return bytes.readRemaining() == 0;
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
         * Call this if you have detected an error condition and you want the context rolled back when it is closed, rather than committed
         */
        @Override
        public void rollbackOnClose() {
            this.rollbackOnClose = true;
        }

        /**
         * Closes the context, committing or rolling back the changes depending on the state.
         */
        @Override
        public void close() {
            close(true);
        }

        /**
         * Close this {@link StoreAppenderContext}, finalizing the writing process and releasing
         * resources. Depending on the conditions, this method either commits the written data,
         * rolls it back, or clears the buffer.
         *
         * @param unlock true if the {@link StoreAppender#writeLock} should be unlocked.
         */
        public void close(boolean unlock) {
            if (!closePreconditionsAreSatisfied()) return;

            try {
                handleInterrupts();
                if (handleRollbackOnClose()) return;

                if (wire == StoreAppender.this.wire) {
                    updateHeaderAndIndex();
                } else if (wire != null) {
                    if (buffered) {
                        writeBytes(wire.bytes());
                        unlock = false;
                        wire.clear();
                    } else {
                        writeBytesInternal(wire.bytes(), metaData);
                        wire = StoreAppender.this.wire;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedRuntimeException(e);
            } catch (StreamCorruptedException | UnrecoverableTimeoutException e) {
                throw new IllegalStateException(e);
            } finally {
                closeCleanup(unlock);
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
                doRollback();
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
            if (chainedElement)
                return false;
            if (isClosed) {
                Jvm.warn().on(getClass(), "Already Closed, close was called twice.", new StackTrace("Second close", closedHere));
                alreadyClosedFound = true;
                return false;
            }
            count--;
            if (count > 0)
                return false;

            if (alreadyClosedFound) {
                closedHere = new StackTrace("Closed here");
            }
            return true;
        }

        /**
         * Historically there have been problems with an interrupted thread causing exceptions in calls below, and we
         * saw half-written messages. If interrupt checking is enabled then check if interrupted and handle
         * appropriately.
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
                wire.updateHeader(positionOfHeader, metaData, 0);
            } catch (IllegalStateException e) {
                if (queue.isClosed())
                    return;
                throw e;
            }

            lastPosition = positionOfHeader;

            if (!metaData) {
                lastIndex(wire.headerNumber());
                store.writePosition(positionOfHeader);
                if (lastIndex != Long.MIN_VALUE) {
                    writeIndexForPosition(lastIndex, positionOfHeader);
                    if (queue.appenderListener != null) {
                        callAppenderListener();
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
            Bytes<?> bytes = wire.bytes();
            bytes.writePositionForHeader(true);
            isClosed = true;
            if (unlock) {
                try {
                    writeLock.unlock();
                } catch (Exception ex) {
                    Jvm.warn().on(getClass(), "Exception while unlocking: ", ex);
                }
            }
        }

        /**
         * Calls the appender listener to process the excerpt at the current position.
         * The read and write positions of the wire are preserved during this operation.
         */
        private void callAppenderListener() {
            final Bytes<?> bytes = wire.bytes();
            long rp = bytes.readPosition();
            long wp = bytes.writePosition();
            try {
                queue.appenderListener.onExcerpt(wire, lastIndex);
            } finally {
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
                assert wire != StoreAppender.this.wire;
                wire.clear();
            } else {
                // zero out all contents...
                final Bytes<?> bytes = wire.bytes();
                try {
                    for (long i = positionOfHeader; i <= bytes.writePosition(); i++)
                        bytes.writeByte(i, (byte) 0);
                    long lastPosition = StoreAppender.this.lastPosition;
                    position0(lastPosition, lastPosition, bytes);
                    ((InternalWire) wire).forceNotInsideHeader();
                } catch (BufferOverflowException | IllegalStateException e) {
                    if (bytes instanceof MappedBytes && ((MappedBytes) bytes).isClosed()) {
                        Jvm.warn().on(getClass(), "Unable to roll back excerpt as it is closed.");
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
                    wire.headerNumber(queue.rollCycle().toIndex(cycle, store.lastSequenceNumber(StoreAppender.this)));
                    long headerNumber0 = wire.headerNumber();
                    assert isInsideHeader(this.wire);
                    return isMetaData() ? headerNumber0 : headerNumber0 + 1;
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
