/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
import net.openhft.chronicle.queue.RollCycle;
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
import java.time.DateTimeException;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.WARN_SLOW_APPENDER_MS;
import static net.openhft.chronicle.wire.MarshallableOut.UNSET_CONTEXT;
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
    @NotNull
    private ContextListenerState contextListenerState;
    private int lastOrdinaryContextCount = -1;
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
        this.contextListenerState = queue.newContextListenerState(this, context);
        this.finalizer = Jvm.isResourceTracing() ? new Finalizer() : null;

        try {
            int lastExistingCycle = queue.lastCycle();
            //! CycleOverflowTest#maximumUInt31CycleIsNotTreatedAsEmpty reopens an appender at Integer.MAX_VALUE;
            //! only the semantic first-cycle value distinguishes that valid roll from an empty Queue here.
            int firstCycle = queue.firstPublishedCycle();
            long start = System.nanoTime();
            int scannedCycle = Integer.MIN_VALUE;
            final WriteLock writeLock = this.queue.writeLock();
            writeLock.lock();
            try {
                // Process cycles and handle EOF markers
                if (firstCycle != UNSET_CONTEXT) {
                    //! constructionSealsEveryRollBelowTheFirstSealedRoll fails if the back-scan excludes the first
                    //! sealed generation. sparseRollsAreTraversedByExistingCycle requires walking actual survivors
                    //! rather than fabricating every numeric cycle, while
                    //! NormaliseEOFsTest#deletingOldestPublishedRollDoesNotBlockAReusedQueue is integration evidence
                    //! for refreshed physical bounds after supported historical deletion; the accepted base already
                    //! permits that deletion but does not use this physical back-scan.
                    final ExistingCycles range = enumerateExistingCycles();
                    firstCycle = range.first;
                    lastExistingCycle = range.last;
                    final NavigableSet<Long> existingCycles = range.cycles;
                    // Backing down until EOF-ed cycle is encountered
                    for (long existingCycle : existingCycles.descendingSet()) {
                        final int eofCycle = Math.toIntExact(existingCycle);
                        setCycle2(eofCycle, WireStoreSupplier.CreateStrategy.READ_ONLY);
                        if (wire == null)
                            break;
                        if (cycleHasEOF()) {
                            // Make sure all older cycles have EOF marker
                            if (eofCycle > firstCycle)
                                normaliseEOFs0(eofCycle);

                            // If first non-EOF file is in the past, it's possible it will be replicated/backfilled to
                            final Long nextExistingCycle = existingCycles.higher(existingCycle);
                            if (nextExistingCycle != null)
                                setCycle2(Math.toIntExact(nextExistingCycle),
                                        WireStoreSupplier.CreateStrategy.READ_ONLY);
                            break;
                        }
                    }
                    if (wire != null)
                        resetPosition();
                    scannedCycle = cycle;

                    // Don't hold the back-scan's store open; the first write re-acquires it
                    releaseParkedStore();
                }
            } finally {
                writeLock.unlock();
                long tookMillis = (System.nanoTime() - start) / 1_000_000;
                if (tookMillis > WARN_SLOW_APPENDER_MS || (lastExistingCycle >= 0 && scannedCycle != lastExistingCycle))
                    Jvm.perf().on(getClass(), "Took " + tookMillis + "ms to find first open cycle " + scannedCycle);
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
        //! SingleChronicleQueueTest#deadAppendLockOwnerDoesNotStrandWriters requires liveness
        //! recovery before rejecting an append after an applying maintenance process has died.
        if (appendLock.locked() && !appendLock.forceUnlockIfProcessIsDead())
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
        contextListenerState = ContextListenerState.NONE;
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
        setCycle2(cycle, createStrategy, true);
    }

    private void setCycle2StrictExisting(final int cycle) {
        //! StoreAppenderTest#currentMappedGenerationDisappearingAfterEnumerationDoesNotAdvanceCursor requires
        //! completion to bypass WireStorePool's same-cycle oldStore shortcut. Otherwise an unlinked POSIX inode can
        //! be resealed through its retained mapping and certified by the cursor even though its pathname disappeared.
        queue.evictCachedCycleMapping(cycle);
        setCycle2(cycle, WireStoreSupplier.CreateStrategy.REINITIALIZE_EXISTING, false);
    }

    private void setCycle2(final int cycle,
                           final WireStoreSupplier.CreateStrategy createStrategy,
                           final boolean reuseCurrentStore) {
        queue.throwExceptionIfClosed();
        if (cycle < 0)
            throw new IllegalArgumentException("You can not have a cycle that starts " +
                    "before Epoch. cycle=" + cycle);

        SingleChronicleQueue queue = this.queue;

        SingleChronicleQueueStore oldStore = this.store;

        SingleChronicleQueueStore newStore = storePool.acquire(
                cycle, createStrategy, reuseCurrentStore ? oldStore : null);
        if (!reuseCurrentStore && newStore == null)
            throw missingPhysicalCycle(cycle);

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
     * Releases the store (and its wires) that the construction-time EOF back-scan left this appender
     * parked on, returning the appender to the same "never written" state as one created on an empty
     * queue. The next write re-acquires the current cycle. Only the mapped-file reservation and open
     * FD are dropped; nothing on disk changes.
     */
    private void releaseParkedStore() {
        if (store == null)
            return;

        releaseBytesFor(wireForIndex);
        releaseBytesFor(wire);
        wireForIndex = null;
        wire = null;

        storePool.closeStore(store);
        store = null;
        cycle = Integer.MIN_VALUE;
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
        if (store == null || wire == null)
            return false;
        try {
            final boolean changed = resetPosition(store.lastSequenceNumber(this));
            assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();
            return changed;
        } catch (StreamCorruptedException e) {
            throw new AssertionError(e);
        }
    }

    private boolean resetPosition(long lastSequenceNumber) {
        long originalHeaderNumber = wire.headerNumber();
        long INVALID_HEADER_NUMBER = -1;

        try {
            if (store == null || wire == null)
                return false;
            long position = store.writePosition();
            position(position, position);

            Bytes<?> bytes = wire.bytes();
            assert !QueueSystemProperties.CHECK_INDEX || checkPositionOfHeader(bytes);

            wire.headerNumber(queue.rollCycle().toIndex(cycle, lastSequenceNumber + 1) - 1);

            assert !QueueSystemProperties.CHECK_INDEX || wire.headerNumber() != INVALID_HEADER_NUMBER ||
                    checkIndex(wire.headerNumber(), positionOfHeader);

            bytes.writeLimit(bytes.capacity());
            return originalHeaderNumber != wire.headerNumber();

        } catch (@NotNull BufferOverflowException e) {
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
    public <T> ExcerptAppender contextListener(@NotNull Class<T> writerType,
                                                @NotNull MarshallableOut.ContextListener<? super T> listener) {
        throwExceptionIfClosed();
        Objects.requireNonNull(writerType, "writerType");
        Objects.requireNonNull(listener, "listener");
        if (queue.doubleBuffer)
            throw new UnsupportedOperationException("contextListener is not supported with double buffering");
        if (contextListenerState.started())
            throw new IllegalStateException("Cannot change contextListener after this appender has written");
        contextListenerState = ContextListenerState.forAppender(this, context, writerType, listener);
        return this;
    }

    @Override
    public int contextCount() {
        if (queue.doubleBuffer)
            throw new IndexNotAvailableException("Context count is unavailable when double buffering because the target cycle is selected when the buffer is flushed");
        return isClosed() ? -1 : lastOrdinaryContextCount;
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
        queue.throwIfContextListenerCallbackActive();
        // we allow the sink process to write metaData
        checkAppendLock(metaData);
        ContextListenerState listenerState = startContextListenerWriteAttempt();
        count++;
        try {
            return prepareAndReturnWriteContext(metaData, listenerState);
        } catch (Throwable e) {
            // Throwable, not just RuntimeException: an Error from a context listener must also
            // restore count, or the next write takes the count>1 fast path and is handed a stale,
            // never-opened context - permanently wedging the appender.
            count--;
            throw Jvm.rethrow(e);
        }
    }

    private ContextListenerState startContextListenerWriteAttempt() {
        ContextListenerState state = contextListenerState;
        if (state == ContextListenerState.UNSET)
            contextListenerState = state = ContextListenerState.NONE;
        state.onWriteAttempt();
        return state;
    }

    /**
     * Prepares and returns the {@link StoreAppenderContext} for writing data. This method checks if
     * the context needs to be reopened, locks the writeLock, handles double buffering if enabled,
     * and ensures the wire and cycle are set correctly for appending.
     *
     * @param metaData indicates if the write context is for metadata
     * @return the prepared {@link StoreAppenderContext} ready for writing
     */
    private StoreAppender.StoreAppenderContext prepareAndReturnWriteContext(
            boolean metaData, ContextListenerState listenerState) {
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
                final long safeLength = queue.overlapSize();
                if (listenerState.requiresDestinationPreflight(metaData)) {
                    resolveOrdinaryAppendDestination();
                    if (listenerState.beforeDocument(false, lastOrdinaryContextCount))
                        resetPosition();
                    assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();
                    openContext(metaData, safeLength, false);
                } else {
                    moveToCycleForAppend();
                    resetPosition();
                    assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();
                    openContext(metaData, safeLength, true);
                    lastOrdinaryContextCount = cycle;
                }

                // Move readPosition to the start of the context. i.e. readRemaining() == 0
                wire.bytes().readPosition(wire.bytes().writePosition());
            } catch (Throwable e) {
                // Catch Throwable, not just RuntimeException: a context listener (or a corrupt-index
                // AssertionError) can throw an Error, and leaking the cross-process write lock would
                // stall every appender in every process until the lock times out.
                writeLock.unlock();
                throw Jvm.rethrow(e);
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
        normaliseEOFs(null);
    }

    void normaliseEOFs(@Nullable Runnable afterCycleEnumeration) {
        long start = System.nanoTime();
        final WriteLock writeLock = queue.writeLock();
        writeLock.lock();
        try {
            //! restartedHistoricalBackfillLowersEofNormalisationBound and
            //! historicalWriteAfterReadyInterruptedRecordNormalisesAtCompletion require the shared cursor to bring
            //! every touched historical roll back into completion. currentTimestampBackfillIsNormalisedOnlyAfterItBecomesHistorical,
            //! futureExactWriteDoesNotAdvanceEofCursorAcrossTimestampCurrentCycle and
            //! backfillBelowRolledBackHighWaterIsNormalisedAtCompletion require the cap to preserve the effective
            //! current/future ordinary-write destination until it becomes historical.
            // CQE retries failed backfills before completion. The shared cursor records the
            // earliest cycle touched by those attempts; completion seals every such historical
            // cycle without sealing the effective ordinary-write destination.
            final int lastPublishedCycle = queue.lastPublishedCycle();
            final int activeCycle = Math.max(queue.cycle(), lastPublishedCycle);
            final int normaliseTo = (int) Math.min((long) lastPublishedCycle + 1, activeCycle);
            //! generationDisappearingAfterEnumerationDoesNotAdvanceCursor uses the package-local
            //! interleaving hook to replace the physical snapshot before the first mutation.
            //! currentMappedGenerationDisappearingAfterEnumerationDoesNotAdvanceCursor additionally requires that
            //! revalidation not reuse an unlinked mapping. They distinguish action-time validation without exposing
            //! the hook through ExcerptAppender.
            normaliseEOFs0(normaliseTo, afterCycleEnumeration);
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
        normaliseEOFs0(cycle, null);
    }

    private void normaliseEOFs0(int cycle, @Nullable Runnable afterCycleEnumeration) {
        final int publishedFirst = queue.firstPublishedCycleWithoutRefresh();
        if (publishedFirst == UNSET_CONTEXT)
            return;
        final LongValue normalisedEOFsTo = queue.tableStoreAcquire(
                NORMALISED_EOFS_TO_TABLESTORE_KEY, publishedFirst);
        final long storedNormalisedEOFsTo = normalisedEOFsTo.getVolatileValue();
        //! corruptedEofNormalisationCursorFailsBeforeMutation requires the persisted long to be validated before it
        //! becomes a Queue cycle or a directory refresh updates listing metadata. Narrowing first can alias a corrupt
        //! value into UInt31 and then report completion while skipping historical rolls; unlike directory metadata,
        //! this cursor has no legacy unset encoding.
        if (storedNormalisedEOFsTo < 0 || storedNormalisedEOFsTo > Integer.MAX_VALUE)
            throw new IllegalStateException("Invalid EOF normalisation cycle " + storedNormalisedEOFsTo
                    + " for queue=" + queue.fileAbsolutePath());

        //! sparseRollsAreTraversedByExistingCycle,
        //! cachedCycleTreeDoesNotHideMalformedPhysicalRoll and
        //! duplicateLogicalCycleFilenameFailsNormalisationWithoutMutation require a fresh snapshot of every physical
        //! generation. Numeric iteration invents gaps, while a listing.modCount-keyed cache can hide external files
        //! and canonical-key filtering can omit aliases before validation.
        final ExistingCycles range = enumerateExistingCycles();
        final int first = range.first;
        if (range.isEmpty())
            return;
        final NavigableSet<Long> existingCycles = range.cycles;
        final int eofCycle = Math.max(first, Math.toIntExact(storedNormalisedEOFsTo));
        if (afterCycleEnumeration != null)
            afterCycleEnumeration.run();
        if (Jvm.isDebugEnabled(StoreAppender.class)) {
            Jvm.debug().on(StoreAppender.class, "Normalising from cycle " + eofCycle);
        }

        //! restartRetriesIncompleteHistoricalRecoveryAndNormalisesEof and
        //! historicalWriteAfterReadyInterruptedRecordNormalisesAtCompletion require each survivor below the cap to
        //! be sealed before advancing the cursor. eofRestorationFailureIsNotReportedAsSuccess,
        //! generationDisappearingAfterEnumerationDoesNotAdvanceCursor and
        //! currentMappedGenerationDisappearingAfterEnumerationDoesNotAdvanceCursor require mutation failure or
        //! disappearance to abort completion; otherwise CQE could acknowledge recovery while a historical roll
        //! remains writable or absent by pathname.
        final int activeCycle = Math.max(queue.cycle(), queue.lastPublishedCycle());
        final int normaliseTo = Math.min(activeCycle, cycle);
        for (long existingCycle : existingCycles) {
            if (existingCycle < eofCycle)
                continue;
            if (existingCycle >= normaliseTo)
                break;

            final int cycleToNormalise = Math.toIntExact(existingCycle);
            setCycle2StrictExisting(cycleToNormalise);
            assert queue.writeLock().locked();
            ensureEndOfData("Unable to normalise end-of-data: queue="
                    + queue.fileAbsolutePath() + ", cycle=" + cycleToNormalise);
        }
        //! sparseRollsAreTraversedByExistingCycle requires the cursor to advance across physically absent gaps only
        //! after every surviving roll below the cap has been sealed. Publishing each observed cycle separately would
        //! repeatedly rescan large sparse ranges; publishing before the loop completed could certify an unsealed roll.
        // The directory enumeration proves that every skipped cycle is absent.
        normalisedEOFsTo.setMaxValue(normaliseTo);
    }

    private ExistingCycles enumerateExistingCycles() {
        RuntimeException initialFailure = null;
        ExistingCycles range = null;
        try {
            range = enumerateExistingCycles0();
        } catch (RuntimeException failure) {
            initialFailure = failure;
        }
        if (range != null && range.hasBothBoundaries())
            return range;

        final boolean initiallyObservedPublishedBounds = range != null && !range.isEmpty();
        try {
            //! cachedCycleTreeDoesNotHideMalformedPhysicalRoll and
            //! duplicateLogicalCycleFilenameFailsNormalisationWithoutMutation require a failed filename scan to
            //! retry the physical snapshot without first publishing directory metadata. A parseable alias can make
            //! refresh() increment listing.modCount even though completion must fail without persistent mutation.
            if (initialFailure == null)
                queue.refreshDirectoryListing();
            range = enumerateExistingCycles0();
        } catch (RuntimeException refreshedFailure) {
            //! malformedPhysicalRollNameFailsNormalisationWithoutAdvancingCursor requires parsing
            //! or enumeration failure to retain its cause after the one permitted refresh. A
            //! fabricated endpoint set could certify completion while omitting touched history.
            if (initialFailure != null)
                refreshedFailure.addSuppressed(initialFailure);
            throw new IllegalStateException("Cannot enumerate physical roll generations after refresh: queue="
                    + queue.fileAbsolutePath(), refreshedFailure);
        }
        requireConsistentRefreshedRange(initiallyObservedPublishedBounds, range,
                queue.fileAbsolutePath(), initialFailure);
        return range;
    }

    private ExistingCycles enumerateExistingCycles0() {
        //! exactEofRecoveryAtMaximumCycleIsRejectedBeforeMutation and
        //! CycleOverflowTest#maximumUInt31CycleIsNotTreatedAsEmpty require physical enumeration to use semantic
        //! publication state. The public firstCycle() maps an empty Queue to Integer.MAX_VALUE and cannot distinguish
        //! that sentinel from the last valid UInt31 cycle.
        final int first = queue.firstPublishedCycleWithoutRefresh();
        if (first == UNSET_CONTEXT)
            return ExistingCycles.empty();
        final int last = queue.lastPublishedCycle();
        try {
            return new ExistingCycles(first, last, queue.listFreshPhysicalCycles());
        } catch (DateTimeException incompatibleRollFilename) {
            //! malformedPhysicalRollNameFailsNormalisationWithoutAdvancingCursor and
            //! nonCanonicalWeeklyRollFailsNormalisationWithoutAdvancingCursor require even a single-file parse or
            //! canonical-name failure to abort. Treating first == last as proof fabricates a generation that was
            //! never validated and can advance the completion cursor falsely.
            throw new IllegalStateException("Cannot enumerate all roll cycles with the persisted roll format: queue="
                    + queue.fileAbsolutePath() + ", first=" + first + ", last=" + last,
                    incompatibleRollFilename);
        }
    }

    static void requireConsistentRefreshedRange(boolean initiallyObservedPublishedBounds,
                                                ExistingCycles range,
                                                String queuePath,
                                                RuntimeException initialFailure) {
        //! refreshedEmptyPhysicalRangeFailsClosed requires a nonempty initial observation that
        //! disappears during refresh to fail without publishing completion. The original failure
        //! remains attached so callers can distinguish stale bounds from malformed names.
        if (range.hasBothBoundaries()
                && !(initiallyObservedPublishedBounds && range.isEmpty()))
            return;
        final IllegalStateException failure = new IllegalStateException(
                "Directory bounds remained inconsistent after refresh: queue=" + queuePath
                        + ", first=" + range.first + ", last=" + range.last
                        + ", observedCycles=" + range.cycles);
        if (initialFailure != null)
            failure.addSuppressed(initialFailure);
        throw failure;
    }

    static final class ExistingCycles {
        private final int first;
        private final int last;
        private final NavigableSet<Long> cycles;

        ExistingCycles(int first, int last, NavigableSet<Long> cycles) {
            this.first = first;
            this.last = last;
            this.cycles = cycles;
        }

        private static ExistingCycles empty() {
            return new ExistingCycles(UNSET_CONTEXT, UNSET_CONTEXT, new TreeSet<>());
        }

        private boolean hasBothBoundaries() {
            return isEmpty()
                    || (!cycles.isEmpty() && cycles.first() == first && cycles.last() == last);
        }

        private boolean isEmpty() {
            return first == UNSET_CONTEXT;
        }
    }

    private void recordBackfillNormalisation(final int recoveredCycle) {
        //! ordinaryAppenderRollsForwardAfterHistoricalIndexedRecovery proves that lowering the completion cursor for
        //! an exact historical write must not lower the ordinary destination; the shared maximum continues to choose
        //! the live roll while a later completion can still reseal the recovered history.
        // Lower the shared cursor even when clock rollback makes this the active high-water cycle.
        // normaliseEOFs0() excludes that active roll; retaining the lower bound lets a later
        // completion seal it after a newer roll is published.
        queue.tableStoreAcquire(NORMALISED_EOFS_TO_TABLESTORE_KEY, recoveredCycle)
                .setMinValue(recoveredCycle);
    }

    /**
     * Ensures the wire is set for the specified cycle, normalizing EOFs as needed.
     * If no wire exists, it creates a new wire for the current cycle.
     *
     * @param cycle the cycle for which the wire should be set
     */
    private void setWireIfNull(final int cycle) {
        setWireIfNull(cycle, WireStoreSupplier.CreateStrategy.CREATE);
    }

    private void setWireIfNull(final int cycle, WireStoreSupplier.CreateStrategy createStrategy) {
        //! unusedAppenderDoesNotCreateDeletedPublishedMaximum requires a caller-selected acquisition strategy: the
        //! previous CREATE-only helper would recreate an absent generation already published in Queue metadata.
        //! stalledAppenderDoesNotRecreateDeletedPublishedMaximum and
        //! unusedAppenderDoesNotCreateDeletedPublishedMaximum require an existing-only target to
        //! be rejected before EOF normalisation can reinterpret its stale physical bounds.
        if (createStrategy == WireStoreSupplier.CreateStrategy.REINITIALIZE_EXISTING
                && !queue.cycleFileExists(cycle))
            throw missingPublishedCycle(cycle);
        normaliseEOFs0(cycle);

        setCycle2(cycle, createStrategy);
        if (store == null)
            throw missingPublishedCycle(cycle);
    }

    /**
     * Moves an ordinary append to the latest cycle known by either time, this appender, or another
     * writer. Time-provider rollback must not move an appender back into a historical roll.
     */
    private void moveToCycleForAppend() {
        //! deletingOldestHistoricalRollPreservesPublishedMaximum and
        //! deletingWholeQueueOfflineAllowsClockSelectedInitialCycle distinguish a retained Queue with a published
        //! high-water mark from a genuinely new Queue. The shared maximum therefore remains the ordinary-write floor
        //! while metadata exists; only a newer target may be created.
        final int publishedCycle = queue.lastPublishedCycle();
        final boolean hasPublishedCycle = publishedCycle != UNSET_CONTEXT;
        // Supported retention keeps the published maximum; taking it with time prevents clock
        // rollback while still allowing a writer to advance normally.
        final int targetCycle = hasPublishedCycle ? Math.max(queue.cycle(), publishedCycle) : queue.cycle();
        final boolean publishedCycleMustExist = hasPublishedCycle && targetCycle == publishedCycle;
        if (wire == null) {
            //! incompletePublishedCycleIsReinitialised, stalledAppenderReinitialisesIncompletePublishedCycle and StoreTailerTest's
            //! shouldHaltAtPartiallyInitialisedRollCycle require REINITIALIZE_EXISTING rather than READ_ONLY:
            //! it still refuses an absent pathname, but preserves Queue's established recovery of an existing
            //! generation whose first header never completed.
            setWireIfNull(targetCycle, publishedCycleMustExist
                    ? WireStoreSupplier.CreateStrategy.REINITIALIZE_EXISTING
                    : WireStoreSupplier.CreateStrategy.CREATE);
            return;
        }

        //! steadyStateAppenderAndTailerReusePublishedCycleStore requires the strict-forward guard: same-cycle writes
        //! must reuse the acquired store rather than repeat store-pool and filesystem work for every document. A
        //! missing published destination is therefore checked when an appender opens or transitions, not by adding
        //! a pathname check to each append on an already-current mapped store.
        if (cycle < targetCycle) {
            if (publishedCycleMustExist)
                requirePublishedCycle(publishedCycle);
            rollCycleTo(targetCycle, false, publishedCycleMustExist);
        }
    }

    private void requirePublishedCycle(int publishedCycle) {
        //! stalledAppenderDoesNotRecreateDeletedPublishedMaximum requires a known-missing destination to fail before
        //! rollCycleTo seals the appender's current store, so the transition needs this separate preflight.
        //! stalledAppenderReinitialisesIncompletePublishedCycle mutation-fails if that probe merely reads the
        //! published generation: an interrupted first header is recoverable, while REINITIALIZE_EXISTING still
        //! returns null instead of creating a genuinely absent pathname.
        try (SingleChronicleQueueStore ignored = queue.pool.acquire(
                publishedCycle, WireStoreSupplier.CreateStrategy.REINITIALIZE_EXISTING, null)) {
            if (ignored == null)
                throw missingPublishedCycle(publishedCycle);
        }
    }

    private IllegalStateException missingPublishedCycle(int publishedCycle) {
        return new IllegalStateException("Highest/current roll " + publishedCycle
                + " disappeared while Queue metadata remains");
    }

    private IllegalStateException missingPhysicalCycle(int missingCycle) {
        return new IllegalStateException("Roll generation disappeared while normalising EOF: queue="
                + queue.fileAbsolutePath() + ", cycle=" + missingCycle);
    }

    /**
     * Writes a header for the current wire, ensuring the correct position and header number
     * is set for the next write operation.
     *
     * @param safeLength the safe length of data that can be written
     * @return the position of the written header
     */
    /**
     * Selects the authoritative ordinary-write destination without opening an application header.
     * QUEUE-144 invokes context listeners only after this method has fixed the actual cycle.
     */
    private void resolveOrdinaryAppendDestination() {
        moveToCycleForAppend();
        resetPosition();
        int header = nextApplicationHeader();
        if (header == END_OF_DATA) {
            advanceOrdinaryAppendCycle();
            resetPosition();
            header = nextApplicationHeader();
        }
        if (header == END_OF_DATA)
            throw new WriteAfterEOFException();
        lastOrdinaryContextCount = cycle;
    }

    private int nextApplicationHeader() {
        positionForNextHeader();
        final Bytes<?> bytes = wire.bytes();
        long position = bytes.writePosition();
        for (; ; ) {
            if (store.dataVersion() > 0)
                position += BytesUtil.padOffset(position);
            final int header = bytes.readVolatileInt(position);
            if (!isReadyMetaData(header))
                return header;
            position += SPB_HEADER_SIZE + lengthOf(header);
        }
    }

    private long writeHeader(final long safeLength) {
        //! canBackfillPreviousCycleAfterEOF and exactWriteReplacesAnIncompleteRequestedEntryWithAWarning require
        //! exact recovery to inspect the next physical header before Wire opens or overwrites it. Ordinary writes
        //! still call this combined helper; splitting positioning from enterHeader changes no ordinary semantics.
        positionForNextHeader();
        return wire.enterHeader(safeLength);
    }

    /**
     * Positions at the next physical header without opening it. Exact recovery must inspect that
     * header before deciding whether it may replace EOF or an incomplete record.
     */
    private void positionForNextHeader() {
        //! exactBackfillFindsEofAfterSecondaryIndexMetadata,
        //! exactBackfillFindsEofAfterTrailingUserMetadata and exactBackfillFindsEofInEmptySealedCycle require the
        //! position to be derived from the published record and then scanned across non-indexed metadata.
        Bytes<?> bytes = wire.bytes();
        // writePosition points at the last record in the queue, so we can just skip it and we're ready for write
        long lastPos = store.writePosition();
        if (positionOfHeader < lastPos) {
            // queue moved since we last touched it - recalculate header number

            try {
                wire.headerNumber(queue.rollCycle().toIndex(cycle, store.lastSequenceNumber(this)));
            } catch (StreamCorruptedException ex) {
                Jvm.warn().on(getClass(), "Couldn't find last sequence", ex);
            }
        }
        final int header = bytes.readVolatileInt(lastPos);
        assert header != NOT_INITIALIZED;
        lastPos += lengthOf(header) + SPB_HEADER_SIZE;
        bytes.writePosition(lastPos);
    }

    /**
     * Inspects the requested index, skipping metadata because it does not consume an index.
     * Opens the requested slot for exact recovery. EOF is deliberately left open so a CQE backfill
     * can append the rest of that cycle without reopening and warning for every entry; the caller
     * must invoke {@link #normaliseEOFs()} before publishing backfill completion.
     */
    private void prepareExactIndexRecovery(final long recoveryIndex) {
        assert writeLock.locked();

        final int recoveryCycle = queue.rollCycle().toCycle(recoveryIndex);
        final Bytes<?> bytes = wire.bytes();
        positionForNextHeader();
        long recoveryPosition = bytes.writePosition();
        for (; ; ) {
            // Exact-index recovery is rejected for legacy unpadded stores before this method.
            recoveryPosition += BytesUtil.padOffset(recoveryPosition);
            final int header = bytes.readVolatileInt(recoveryPosition);
            if (header == NOT_INITIALIZED) {
                //! exactWriteInitializesUnusedRequestedEntryWithoutWarning is integration-preservation evidence that
                //! an unused exact-next slot opens without a recovery warning while still entering the completion
                //! range; the accepted base already writes an unused slot, but does not record that obligation.
                recordBackfillNormalisation(recoveryCycle);
                bytes.writePosition(recoveryPosition);
                return;
            }
            if (header == END_OF_DATA) {
                //! exactEofRecoveryAtMaximumCycleIsRejectedBeforeMutation fails if exact recovery opens the final
                //! UInt31 roll. There is no successor which can make that roll historical, so completion's active
                //! cycle exclusion cannot safely reseal it. The read-only preflight normally rejects first; this
                //! check remains as fail-closed protection against a non-cooperating change after that inspection.
                if (recoveryCycle == Integer.MAX_VALUE)
                    throw maximumCycleRecoveryFailure(recoveryIndex);
                //! canBackfillPreviousCycleAfterEOF requires only the observed EOF word to be reopened, with the
                //! touched cycle recorded before mutation so completion can reseal it after a later failure.
                recordBackfillNormalisation(recoveryCycle);
                bytes.writePosition(recoveryPosition);
                replaceEndOfDataMarkerForRecovery(bytes, recoveryPosition);
                warnExactIndexRecovery("reopened end-of-data", recoveryIndex,
                        recoveryPosition, header);
                return;
            }
            if ((header & NOT_COMPLETE) != 0) {
                //! exactWriteReplacesAnIncompleteRequestedEntryAfterQueueRestart requires the exact-next incomplete
                //! header to be cleared in place and warned once; it is neither a published duplicate nor corruption.
                recordBackfillNormalisation(recoveryCycle);
                bytes.writePosition(recoveryPosition);
                warnExactIndexRecovery("replaced incomplete header", recoveryIndex,
                        recoveryPosition, header);
                // Clear the failed header here so Wire.enterHeader does not emit a second warning.
                bytes.writeVolatileInt(recoveryPosition, NOT_INITIALIZED);
                return;
            }
            if (isReadyMetaData(header)) {
                //! exactBackfillCanAddASecondaryIndexBeforeResealing and
                //! exactBackfillFindsEofAfterTrailingUserMetadata require ready metadata to consume physical bytes
                //! without consuming the requested Queue index.
                recoveryPosition += SPB_HEADER_SIZE + lengthOf(header);
                continue;
            }
            if (isReadyData(header)) {
                //! The exactIndexStateWithoutMutation() preflight and capped publishUnpublishedRecords() consume
                //! every ready record needed by a valid request while the shared write lock is held, so no
                //! cooperative test can reach this branch. Retain the failure for mapped-file corruption or a
                //! writer which bypasses that lock; treating the record as a free slot would overwrite ready data.
                // publishUnpublishedRecords() runs before this scan while the write lock is
                // held, so ready data beyond the published position indicates corruption.
                throw new IllegalStateException("Ready data record after the published write position at "
                        + recoveryPosition + " in " + queue.fileAbsolutePath());
            }
            //! Cooperative writers cannot produce another terminal word while the Queue write lock is held, so no
            //! current test discriminates this residual corruption guard. Treating an unknown ready/reserved word as
            //! a free slot would overwrite physical state whose Queue-index meaning has not been established.
            throw new IllegalStateException("Unexpected recovery header 0x" + Integer.toHexString(header));
        }
    }

    private IllegalStateException maximumCycleRecoveryFailure(final long recoveryIndex) {
        return new IllegalStateException("Cannot reopen end-of-data in the final UInt31 cycle for exact-index "
                + "recovery: queue=" + queue.fileAbsolutePath()
                + ", index=0x" + Long.toHexString(recoveryIndex));
    }

    private void warnExactIndexRecovery(final String action, final long recoveryIndex,
                                        final long recoveryPosition, final int header) {
        final int recoveryCycle = queue.rollCycle().toCycle(recoveryIndex);
        Jvm.warn().on(getClass(), "Exact-index recovery " + action + ": queue="
                + queue.fileAbsolutePath()
                + ", cycle=" + recoveryCycle
                + ", index=0x" + Long.toHexString(recoveryIndex)
                + ", position=" + recoveryPosition
                + ", header=0x" + Integer.toHexString(header));
    }

    /**
     * Performs the single atomic mutation that opens a sealed roll for exact-index recovery.
     * Package visibility permits the failed-CAS invariant to be tested without reflection.
     */
    static void replaceEndOfDataMarkerForRecovery(Bytes<?> bytes, long recoveryPosition) {
        //! StoreAppenderTest#exactRecoveryFailsIfTheEofMarkerChangesBeforeCas directly discriminates the expected-word
        //! compare-and-swap. An unconditional write could reopen a header changed after inspection and overwrite a
        //! concurrent or corrupt terminal state instead of failing closed.
        if (!bytes.compareAndSwapInt(recoveryPosition, END_OF_DATA, NOT_INITIALIZED))
            throw new IllegalStateException("End-of-data changed while starting exact-index recovery at "
                    + recoveryPosition);
    }

    private void advanceOrdinaryAppendCycle() {
        //! eofAdvanceRejectsCycleOverflowBeforeMutation requires overflow rejection before roll creation or
        //! publication. The source already reported END_OF_DATA, so rollCycleTo suppresses its ordinary reseal;
        //! the flag is a defensive format invariant rather than an independently discriminated branch.
        // Reject overflow before creating or publishing a roll. The triggering EOF already seals
        // the source generation, so this transition must not write a second seal there.
        if (cycle == Integer.MAX_VALUE)
            throw new IllegalStateException("Cannot advance ordinary append beyond cycle " + cycle);
        rollCycleTo(cycle + 1, true);
    }

    /**
     * Opens an ordinary header. END_OF_DATA advances to cycle + 1 exactly once. A second EOF
     * indicates inconsistent state and is propagated. Exact-index writes do not use this path.
     */
    private long writeHeaderForOrdinaryAppend(final long safeLength) {
        //! ordinaryWritingDocumentRollsForwardPastSealedCurrentCycle and
        //! sequentialWriteBytesRollsForwardPastSealedCurrentCycle require the same single retry for both ordinary
        //! entry points. They establish an observed END_OF_DATA boundary, not crash durability.
        try {
            return writeHeader(safeLength);
        } catch (WriteAfterEOFException ignored) {
            //! Current Wire clears insideHeader before throwing, so tests against that dependency cannot discriminate
            //! this compatibility call. Older binary-compatible Wire versions leave the transient state set; clearing
            //! it before cycle selection prevents a subsequent rollover failure from stranding this appender. Remove
            //! the fallback when the minimum Wire version guarantees its own cleanup.
            // Clear transient acquisition state without altering the durable EOF before selecting the next roll.
            ((InternalWire) wire).forceNotInsideHeader();
            advanceOrdinaryAppendCycle();
            try {
                return writeHeader(safeLength);
            } catch (WriteAfterEOFException secondEOF) {
                //! secondConsecutiveEofIsPropagated fails if the retry becomes a loop and verifies that cleanup
                //! leaves the appender reusable after the second observed seal is propagated.
                ((InternalWire) wire).forceNotInsideHeader();
                throw secondEOF;
            }
        }
    }

    /**
     * Opens a new write context for appending data, setting up the necessary parameters such as
     * the header, write position, and metadata flag.
     *
     * @param metaData         indicates if the context is for metadata
     * @param safeLength       the maximum length of data that can be safely written
     * @param rollAtEndOfData  whether this ordinary append may advance once past a sealed roll;
     *                         exact-index writes pass {@code false} and remain strict
     */
    private void openContext(final boolean metaData, final long safeLength, final boolean rollAtEndOfData) {
        assert wire != null;
        //! Exact-index recovery must remain strict while ordinary documents may cross one EOF. Keeping the policy as
        //! an explicit argument prevents writeBytesInternal() from accidentally inheriting the ordinary retry when
        //! the two paths share context construction.
        this.positionOfHeader = rollAtEndOfData
                ? writeHeaderForOrdinaryAppend(safeLength)
                : writeHeader(safeLength);
        context.isClosed = false;
        context.rollbackOnClose = false;
        context.buffered = false;
        context.wire = wire; // Jvm.isDebug() ? acquireBufferWire() : wire;
        context.metaData(metaData);
    }

    /**
     * Opens a document for a context listener while this appender's write lock is already held.
     *
     * <p>This is package-private solely for {@link ContextListenerState}. Context
     * listeners cannot use the regular appender entry points because those paths attempt to acquire
     * the non-reentrant write lock again.</p>
     *
     * @param metaData whether the listener document contains metadata
     */
    void openContextForContextListener(boolean metaData) {
        resetPosition();
        openContext(metaData, queue.overlapSize(), false);
    }

    /**
     * Closes the document written by a context listener without releasing the appender's write
     * lock, which remains owned by the outer application write.
     *
     * <p>The temporary count prevents any nesting state from the application write from suppressing
     * the listener document's commit. The original count is restored for the application document
     * that follows.</p>
     */
    void closeContextForContextListener() {
        int savedCount = count;
        try {
            count = 1;
            context.close(false);
        } finally {
            count = savedCount;
        }
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
            //! eosOnlyRestartPreservesReadySequenceZeroBeforeOrdinaryAppend proves zero can also mean a ready first
            //! record whose write position was not published. Let lastSequenceNumber() perform its physical scan
            //! instead of asserting against the empty-store sentinel.
            if (pos == 0)
                return true;

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
        queue.throwIfContextListenerCallbackActive();
        checkAppendLock();
        ContextListenerState listenerState = startContextListenerWriteAttempt();
        writeLock.lock();
        try {
            if (listenerState.requiresDestinationPreflight(false)) {
                resolveOrdinaryAppendDestination();
                if (listenerState.beforeRawDocument(lastOrdinaryContextCount))
                    resetPosition();
                this.positionOfHeader = writeHeader(queue.overlapSize());
            } else {
                moveToCycleForAppend();
                this.positionOfHeader = writeHeaderForOrdinaryAppend(queue.overlapSize());
                lastOrdinaryContextCount = cycle;
            }

            assert isInsideHeader(wire);
            beforeAppend(wire, wire.headerNumber() + 1);
            Bytes<?> wireBytes = wire.bytes();
            wireBytes.write(bytes);
            wire.updateHeader(positionOfHeader, false, 0);
            //! writeBytesAndIndexFiveTimesTest and testIndexQueue require ordinary byte appends to publish local
            //! position, mapped writePosition and sparse index through the same ordering used by exact recovery.
            recordCommittedData(wire.headerNumber(), positionOfHeader);
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
        queue.throwIfContextListenerCallbackActive();
        checkAppendLock();
        writeLock.lock();
        try {
            writeBytesInternal(index, bytes);
        } finally {
            writeLock.unlock();
        }
    }

    void ensureEndOfData(final String failureMessage) {
        //! eofRestorationFailureIsNotReportedAsSuccess fails if a false writeEOF result is accepted without proving
        //! that another writer installed EOF. Completion must either observe that seal or propagate failure.
        if (store.writeEOF(wire, timeoutMS()) || cycleHasEOF())
            return;
        throw new IllegalStateException(failureMessage);
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

        //! exactRecoveryRejectsUnsupportedSequenceBeforeCreatingRoll and
        //! rejectedExactGapIntoExistingLaterRollDoesNotSealTheCurrentRoll require capacity and gap classification
        //! before roll selection can create a file, seal the current roll or publish a cycle.
        //! ordinaryAppenderContinuesInCurrentCycleAfterIndexedWriteToUnsealedRoll is integration-preservation
        //! evidence that a valid exact write does not disturb the ordinary destination; the accepted base also
        //! keeps that destination.
        final RollCycle rollCycle = queue.rollCycle();
        final int cycle = rollCycle.toCycle(index);
        final long sequenceNumber = rollCycle.toSequenceNumber(index);
        if (sequenceNumber >= rollCycle.maxMessagesPerCycle())
            throw new IllegalArgumentException("Exact-index sequence " + sequenceNumber
                    + " is outside maxMessagesPerCycle=" + rollCycle.maxMessagesPerCycle()
                    + " for index=0x" + Long.toHexString(index));

        //! sameCycleGapDoesNotPublishReadyCrashRecord and
        //! publishedDuplicateDoesNotAdoptLaterReadyCrashRecord require every exact request, including one for the
        //! appender's current roll, to classify published and physical state through a read-only view first. Deferring
        //! this until after resetPosition() and crash-record adoption makes a rejected gap or old duplicate publish
        //! unrelated ready data.
        final ExactIndexState exactIndexState = exactIndexStateWithoutMutation(
                cycle, sequenceNumber, bytes);
        //! exactWriteDoesNotRecreateDeletedPublishedMaximum and
        //! exactWriteRejectsAbsentCycleWithinPublishedRange require absence to remain distinct from an existing
        //! empty store. The published maximum is authoritative, and an absent interior generation is ambiguous
        //! between a deliberate sparse gap and unsupported deletion; only targets outside the retained bounds may
        //! be created without persisted per-cycle provenance.
        if (!exactIndexState.targetExists)
            requireAbsentExactTargetCanBeCreated(cycle);
        //! publishedDuplicatePayloadsAreComparedWithoutMutation and CreateAtIndexTest#testWriteBytesWithIndex require
        //! a published duplicate to be compared before returning: equal content is debug-only, while different
        //! content warns with both hex dumps and is ignored. Comparing after roll selection could seal or move the
        //! source appender merely to diagnose an already-applied index.
        //! StoreAppenderInternalWriteBytesTest#internalWriteBytesShouldBeIdempotentUnderConcurrentUpdates,
        //! #internalWriteBytesShouldBeIdempotent and WriteBytesIndexTest#writeMultipleAppenders retain the broader
        //! independent-Queue, per-entry reopen and live-tailer compatibility coverage.
        //! independentQueueExactWritersReplayAcrossCyclesAndRestart adds deterministic sparse-cycle completion and
        //! full Queue-object restart coverage. Together with CreateAtIndexTest#testWriteBytesWithIndex, these tests
        //! consolidate the narrower deleted cannotOverwriteExistingEntries_DifferentQueueInstance and
        //! writeBytesAndIndexFiveTimesWithOverwriteTest cases. They are integration-preservation tests rather than
        //! accepted-base discriminators. In every case the first published record remains authoritative.
        if (exactIndexState.hasPublishedIndex()
                && index <= exactIndexState.publishedIndex(rollCycle, cycle))
            return;

        final long nextIndexInTarget = exactIndexState.nextPhysicalIndex(rollCycle, cycle);
        //! InternalAppenderWriteBytesTest#cannotWriteToNonZeroIndexOfNewRollCycleWithoutMutation and
        //! rejectedExactGapIntoExistingLaterRollDoesNotSealTheCurrentRoll require the physical next-index comparison
        //! beside the rejection itself and before roll selection. Deferring it could create the target or seal and
        //! move the source appender even though the requested sequence leaves a gap.
        if (index > nextIndexInTarget)
            throw new IllegalIndexException(index, nextIndexInTarget - 1);

        //! exactEofRecoveryAtMaximumCycleIsRejectedBeforeMutation requires the terminal EOF decision to be made
        //! through the read-only preflight. Reaching prepareExactIndexRecovery() first could move this appender and
        //! lower the shared completion cursor even though the final UInt31 roll can never become historical.
        if (cycle == Integer.MAX_VALUE
                && index == nextIndexInTarget
                && exactIndexState.terminalHeader == END_OF_DATA)
            throw maximumCycleRecoveryFailure(index);

        if (wire == null)
            setWireIfNull(cycle);

        // If the header number has changed, the appender has rolled.
        if (this.cycle != cycle)
            rollCycleTo(cycle, this.cycle > cycle);

        requirePaddedExactStore(store, cycle);

        //! exactPreflightRejectsStaleAliasingEncodedSequence requires mutation to retain the full-position result
        //! from preflight. Calling the general resetPosition() here would re-read the lossy paired sequence and could
        //! reject a valid exact-next request after the read-only classification had already proved its boundary.
        resetPosition(exactIndexState.lastPublishedSequence);
        final long adoptedIndex = publishUnpublishedRecords(cycle, index, exactIndexState);
        //! exactWriteAdoptsReadyFirstRecordRetryBeforeWritingNext and
        //! restartPublishesReadyRecordLeftBeyondWritePosition require ready crash records to be published before
        //! opening an exact-next slot. Capping publication at the requested index prevents an older ready duplicate
        //! from adopting unrelated later records; gap and published-duplicate classification already completed
        //! without mutation.
        if (index == adoptedIndex)
            return;

        long headerNumber = wire.headerNumber();

        //! The read-only preflight and this recheck execute under the shared Queue write lock, so a cooperative test
        //! cannot change headerNumber between them. Retain the bounds for a non-cooperating mapped writer: accepting a
        //! later header would skip a gap, while overwriting an earlier one would violate first-writer authority.
        if (index > headerNumber + 1)
            throw new IllegalIndexException(index, headerNumber);
        //! The shared lock makes publication after preflight unreachable for cooperating writers, so no deterministic
        //! Queue test discriminates this fallback. A writer bypassing that lock can still publish first; compare its
        //! authoritative record and ignore the supplied duplicate rather than overwrite it or throw.
        if (index <= headerNumber) {
            comparePublishedEntry(store, wire, cycle, sequenceNumber, bytes);
            return;
        }

        prepareExactIndexRecovery(index);

        writeBytesInternal(bytes, false);
        //assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();

        headerNumber = wire.headerNumber();
        boolean isIndex = index == headerNumber;
        if (!isIndex) {
            throw new IllegalStateException("index: " + index + ", header: " + headerNumber);
        }

    }

    /**
     * Publishes ready data left beyond the store write position when a writer stopped between
     * making a header ready and {@link #recordCommittedData(long, long)}. Publication stops at the
     * requested ready record, or at the final predecessor of an exact-next request, so replaying an
     * older index cannot publish unrelated later data.
     */
    private long publishUnpublishedRecords(final int cycle,
                                           final long requestedIndex,
                                           final ExactIndexState exactIndexState) {
        //! exactWriteAdoptsReadyFirstRecordRetryBeforeWritingNext and
        //! restartPublishesReadyRecordLeftBeyondWritePosition cover interruptions where a ready record precedes
        //! writePosition publication. Publish only through the requested ready record, or through all contiguous
        //! predecessors of an exact-next request. Publishing every physical record would let an old duplicate adopt
        //! unrelated later data. General sparse-index repair remains out of scope.
        final RollCycle rollCycle = queue.rollCycle();
        final long requestedSequence = rollCycle.toSequenceNumber(requestedIndex);
        final long sequenceToPublish = Math.min(requestedSequence, exactIndexState.lastPhysicalSequence);
        if (sequenceToPublish <= exactIndexState.lastPublishedSequence)
            return Long.MIN_VALUE;

        final Bytes<?> bytes = wire.bytes();
        final long publishedPosition = store.writePosition();
        //! The Queue write lock prevents a cooperative discriminator for an action-time publication change. Retain
        //! this comparison for a non-cooperating mapped writer; indexing from a stale position could publish the wrong
        //! physical record after preflight had classified another boundary.
        if (publishedPosition != exactIndexState.publishedPosition)
            throw new IllegalStateException("Exact-index target changed after preflight: queue="
                    + queue.fileAbsolutePath() + ", cycle=" + cycle);
        long position = publishedPosition
                + lengthOf(bytes.readVolatileInt(publishedPosition)) + SPB_HEADER_SIZE;
        long lastReadyDataPosition = -1;
        long readySequence = exactIndexState.lastPublishedSequence;
        for (; ; ) {
            position += BytesUtil.padOffset(position);
            final int header = bytes.readVolatileInt(position);
            if (isReadyData(header)) {
                lastReadyDataPosition = position;
                if (++readySequence == sequenceToPublish)
                    break;
            } else if (!isReadyMetaData(header)) {
                break;
            }
            position += SPB_HEADER_SIZE + lengthOf(header);
        }
        //! The same lock prevents a cooperative test from changing the scanned ready-record prefix. Fail closed if a
        //! non-cooperating writer does so: publishing a shorter or different prefix would assign the requested Queue
        //! sequence to physical data that preflight did not validate.
        if (lastReadyDataPosition < 0 || readySequence != sequenceToPublish)
            throw new IllegalStateException("Exact-index physical state changed after preflight: queue="
                    + queue.fileAbsolutePath() + ", cycle=" + cycle);

        final long lastIndex = rollCycle.toIndex(cycle, readySequence);
        try {
            recordBackfillNormalisation(cycle);
            wire.headerNumber(lastIndex);
            recordCommittedData(lastIndex, lastReadyDataPosition);
            return lastIndex;
        } catch (StreamCorruptedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Reads the publication boundary, contiguous physical records and terminal header through a
     * private read-only view, so every duplicate or gap can be classified before this appender
     * rolls, seals or publishes anything.
     */
    private ExactIndexState exactIndexStateWithoutMutation(final int cycle,
                                                           final long requestedSequence,
                                                           @NotNull final BytesStore<?, ?> suppliedBytes) {
        final RollCycle rollCycle = queue.rollCycle();
        try (SingleChronicleQueueStore target = queue.storeForCycle(cycle, queue.epoch(), false, null)) {
            if (target == null)
                return ExactIndexState.absent();
            requirePaddedExactStore(target, cycle);
            try (MappedBytes bytes = target.bytes()) {
                final long publishedPosition = target.writePosition();
                final Wire preflightWire = queue.wireType().apply(bytes);
                preflightWire.usePadding(target.dataVersion() > 0);
                final long lastPublishedSequence = target.lastPublishedSequenceNumber(preflightWire);
                //! Supported publication cannot produce an out-of-capacity sequence, so no cooperative test reaches
                //! this guard. Validate the persisted scan result before composing a Queue index; accepting a corrupt
                //! negative or oversized value could alias the exact-next boundary and authorize the wrong slot.
                if (lastPublishedSequence < -1
                        || lastPublishedSequence >= rollCycle.maxMessagesPerCycle())
                    throw new IllegalStateException("Invalid published sequence " + lastPublishedSequence
                            + " in cycle " + cycle + " for queue=" + queue.fileAbsolutePath());

                if (requestedSequence <= lastPublishedSequence) {
                    comparePublishedEntry(target, preflightWire, cycle, requestedSequence, suppliedBytes);
                    return new ExactIndexState(true, publishedPosition, lastPublishedSequence,
                            lastPublishedSequence, NOT_INITIALIZED);
                }

                long lastPhysicalSequence = lastPublishedSequence;
                long position = publishedPosition
                        + lengthOf(bytes.readVolatileInt(publishedPosition)) + SPB_HEADER_SIZE;
                for (; ; ) {
                    position += BytesUtil.padOffset(position);
                    final int header = bytes.readVolatileInt(position);
                    if (isReadyData(header)) {
                        //! No supported writer can place more ready records than maxMessagesPerCycle, so there is no
                        //! cooperative discriminator. Stop before incrementing beyond the declared sequence domain;
                        //! otherwise corrupt physical data could wrap or authorize an unsupported exact index.
                        if (++lastPhysicalSequence >= rollCycle.maxMessagesPerCycle())
                            throw new IllegalStateException("Physical sequence exceeds maxMessagesPerCycle in cycle "
                                    + cycle + " for queue=" + queue.fileAbsolutePath());
                    } else if (!isReadyMetaData(header)) {
                        return new ExactIndexState(true, publishedPosition, lastPublishedSequence,
                                lastPhysicalSequence, header);
                    }
                    position += SPB_HEADER_SIZE + lengthOf(header);
                }
            }
        }
    }

    private void comparePublishedEntry(@NotNull final SingleChronicleQueueStore target,
                                       @NotNull final Wire comparisonWire,
                                       final int cycle,
                                       final long sequenceNumber,
                                       @NotNull final BytesStore<?, ?> suppliedBytes) {
        final Bytes<?> existingBytes = comparisonWire.bytes();
        final long savedReadPosition = existingBytes.readPosition();
        final long savedReadLimit = existingBytes.readLimit();
        final long savedWritePosition = existingBytes.writePosition();
        try {
            final ExcerptContext comparisonContext = new WireExcerptContext(comparisonWire, timeoutMS());
            final ScanResult scanResult = target.moveToIndexForRead(comparisonContext, sequenceNumber);
            if (scanResult != ScanResult.FOUND) {
                warnUnableToComparePublishedEntry(cycle, sequenceNumber, suppliedBytes, scanResult);
                return;
            }
            comparePublishedEntry(cycle, sequenceNumber, existingBytes, suppliedBytes);

        } finally {
            existingBytes.readPosition(savedReadPosition);
            existingBytes.readLimit(savedReadLimit);
            existingBytes.writePosition(savedWritePosition);
        }
    }

    private void comparePublishedEntry(final int cycle,
                                       final long sequenceNumber,
                                       @NotNull final Bytes<?> existingBytes,
                                       @NotNull final BytesStore<?, ?> suppliedBytes) {
        final int header = existingBytes.readVolatileInt();
        assert isReadyData(header);
        final int existingLength = lengthOf(header);
        final long suppliedLength = suppliedBytes.readRemaining();
        final long index = queue.rollCycle().toIndex(cycle, sequenceNumber);
        //! InternalAppenderWriteBytesTest#publishedDuplicatePayloadsAreComparedWithoutMutation distinguishes both
        //! outcomes. Equal content is debug-only (and therefore silent when debug is disabled); different content
        //! warns with both hex dumps, but neither path throws or overwrites the authoritative published record.
        if (existingLength == suppliedLength
                && existingBytes.equalBytes(suppliedBytes, existingLength)) {
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), "Exact-index duplicate matches published content and was ignored: queue="
                        + queue.fileAbsolutePath() + ", cycle=" + cycle + ", index=0x" + Long.toHexString(index)
                        + ", length=" + existingLength);
            return;
        }

        Jvm.warn().on(getClass(), "Exact-index duplicate differs from published content and was ignored: queue="
                + queue.fileAbsolutePath() + ", cycle=" + cycle + ", index=0x" + Long.toHexString(index)
                + ", existingLength=" + existingLength + ", suppliedLength=" + suppliedLength
                + "\nexisting:\n" + existingBytes.toHexString(existingLength)
                + "\nsupplied:\n" + toHexDump(suppliedBytes, suppliedLength));
    }

    private void warnUnableToComparePublishedEntry(final int cycle,
                                                   final long sequenceNumber,
                                                   @NotNull final BytesStore<?, ?> suppliedBytes,
                                                   @NotNull final ScanResult reason) {
        //! A cooperative Queue cannot publish an index which its own read path cannot find, so no deterministic valid
        //! input reaches this branch. Preserve the non-throwing duplicate contract while warning that corruption or a
        //! non-cooperating writer prevented the requested comparison; the supplied payload remains ignored.
        final long index = queue.rollCycle().toIndex(cycle, sequenceNumber);
        Jvm.warn().on(getClass(), "Exact-index duplicate could not be compared and was ignored: queue="
                + queue.fileAbsolutePath() + ", cycle=" + cycle + ", index=0x" + Long.toHexString(index)
                + ", suppliedLength=" + suppliedBytes.readRemaining() + ", reason=" + reason
                + "\nsupplied:\n" + toHexDump(suppliedBytes, suppliedBytes.readRemaining()));
    }

    private static String toHexDump(@NotNull final BytesStore<?, ?> bytesStore,
                                    final long length) {
        if (bytesStore instanceof Bytes)
            return ((Bytes<?>) bytesStore).toHexString(length);

        final Bytes<?> bytes = bytesStore.bytesForRead();
        try {
            bytes.readPosition(bytesStore.readPosition());
            return bytes.toHexString(length);
        } finally {
            bytes.releaseLast();
        }
    }

    private static final class WireExcerptContext implements ExcerptContext {
        @NotNull
        private final Wire wire;
        private final long timeoutMS;

        private WireExcerptContext(@NotNull final Wire wire, final long timeoutMS) {
            this.wire = wire;
            this.timeoutMS = timeoutMS;
        }

        @Override
        public Wire wire() {
            return wire;
        }

        @Override
        public Wire wireForIndex() {
            return wire;
        }

        @Override
        public long timeoutMS() {
            return timeoutMS;
        }
    }

    private static final class ExactIndexState {
        private final boolean targetExists;
        private final long publishedPosition;
        private final long lastPublishedSequence;
        private final long lastPhysicalSequence;
        private final int terminalHeader;

        private ExactIndexState(boolean targetExists,
                                long publishedPosition,
                                long lastPublishedSequence,
                                long lastPhysicalSequence,
                                int terminalHeader) {
            this.targetExists = targetExists;
            this.publishedPosition = publishedPosition;
            this.lastPublishedSequence = lastPublishedSequence;
            this.lastPhysicalSequence = lastPhysicalSequence;
            this.terminalHeader = terminalHeader;
        }

        private static ExactIndexState absent() {
            return new ExactIndexState(false, 0, -1, -1, NOT_INITIALIZED);
        }

        private boolean hasPublishedIndex() {
            return lastPublishedSequence >= 0;
        }

        private long publishedIndex(RollCycle rollCycle, int cycle) {
            return rollCycle.toIndex(cycle, lastPublishedSequence);
        }

        private long nextPhysicalIndex(RollCycle rollCycle, int cycle) {
            return rollCycle.toIndex(cycle, lastPhysicalSequence + 1);
        }
    }

    private void requireAbsentExactTargetCanBeCreated(final int targetCycle) {
        final int publishedLast = queue.lastPublishedCycle();
        if (publishedLast == UNSET_CONTEXT)
            return;
        final int publishedFirst = queue.firstPublishedCycleWithoutRefresh();
        if (targetCycle < publishedFirst || targetCycle > publishedLast)
            return;
        if (targetCycle == publishedLast)
            throw missingPublishedCycle(targetCycle);
        throw new IllegalStateException("Cannot create absent exact-index cycle " + targetCycle
                + " within retained published range " + publishedFirst + ".." + publishedLast
                + " for queue=" + queue.fileAbsolutePath());
    }

    private void requirePaddedExactStore(final SingleChronicleQueueStore target, final int cycle) {
        //! exactRecoveryRejectsLegacyUnpaddedStore and
        //! crossCycleExactRecoveryRejectsLegacyTargetBeforeRollover require exact-index recovery
        //! to reject the target's unpadded physical format before roll selection or mutation.
        //! They do not exercise SingleChronicleQueueStore's replacement-Wire EOF fallback.
        if (target.dataVersion() == 0)
            throw new UnsupportedOperationException("Exact-index recovery is not supported for "
                    + "legacy unpadded queue stores: queue=" + queue.fileAbsolutePath()
                    + ", cycle=" + cycle);
    }

    private void writeBytesInternal(@NotNull final BytesStore<?, ?> bytes, boolean metadata) {
        assert writeLock.locked();
        try {
            assert count == 0 : "count=" + count;
            //! canBackfillPreviousCycleAfterEOF and exactBackfillFindsEofInEmptySealedCycle require exact recovery to
            //! handle the intended physical EOF before this call. Passing false keeps Q2's ordinary successor-cycle
            //! retry out of the exact path; enabling it here would report success at an index the caller did not
            //! request and canBackfillPreviousCycleAfterEOF would no longer remain in the recovered cycle.
            openContext(metadata, safeLength, false);

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
        rollCycleTo(cycle, suppressEOF, false);
    }

    private void rollCycleTo(final int cycle, boolean suppressEOF, boolean existingOnly) {

        // only a valid check if the wire was set.
        if (this.cycle == cycle)
            throw new AssertionError();

        if (!suppressEOF) {
            assert queue.writeLock().locked();
            store.writeEOF(wire, timeoutMS());
        }

        //! stalledWriterSeesCyclePublishedByAnotherJvmWithoutRefreshingDirectoryListing fails if rollover calls
        //! lastCycle(): a directory refresh can replace the cooperating writer's destination with an unrelated
        //! unreported filename and adds filesystem I/O to the append path. testCountExcerptsWhenTheCycleIsRolled,
        //! testRollCycle and testRead2 pin the corresponding publication and modification-count effects of using the
        //! mapped maximum here.
        int lastPublishedCycle = queue.lastPublishedCycle();

        // If we're behind the target cycle, roll forward to the last existing cycle first
        if (lastPublishedCycle != UNSET_CONTEXT
                && lastPublishedCycle < cycle
                && lastPublishedCycle != this.cycle) {
            setCycle2(lastPublishedCycle, WireStoreSupplier.CreateStrategy.READ_ONLY);
            if (store == null)
                throw missingPublishedCycle(lastPublishedCycle);
            rollCycleTo(cycle);
        } else {
            //! stalledAppenderDoesNotRecreateDeletedPublishedMaximum requires an equality transition to open the
            //! published target without CREATE; otherwise a removed generation is silently replaced after the
            //! source store has been sealed.
            setCycle2(cycle, existingOnly
                    ? WireStoreSupplier.CreateStrategy.READ_ONLY
                    : WireStoreSupplier.CreateStrategy.CREATE);
            if (existingOnly && store == null)
                throw missingPublishedCycle(cycle);
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

    private void recordCommittedData(final long index, final long position) throws StreamCorruptedException {
        //! testIndexQueue and writeBytesAndIndexFiveTimesTest pin the common final state for ordinary, context and
        //! exact writes. Keeping one helper prevents those paths publishing different subsets; the tests do not
        //! independently stop between assignments or establish crash durability for this precise order.
        lastPosition = position;
        lastIndex(index);
        store.writePosition(position);
        writeIndexForPosition(index, position);
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

            if (!metaData) {
                //! writeBytesAndIndexFiveTimesTest exercises the context-close route as well as direct bytes. Reuse
                //! recordCommittedData() for data only; metadata consumes physical space but must not publish a queue
                //! index or writePosition as application data.
                //! eosOnlyRestartPreservesReadySequenceZeroBeforeOrdinaryAppend also requires this path to publish
                //! sequence zero: updateHeader() has made the data header ready and assigned its queue index, while a
                //! lastIndex sentinel guard here would hide the first valid record from restart recovery.
                recordCommittedData(wire.headerNumber(), positionOfHeader);
                if (queue.appenderListener != null) {
                    callAppenderListener();
                }
            } else {
                lastPosition = positionOfHeader;
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

        @Override
        public int contextCount() {
            // Reject on any double-buffered queue, not just when this write happened to hit lock
            // contention: otherwise the same code works or throws depending on runtime contention.
            // Progressive contextCount usage and double buffering are an unsupported combination.
            if (queue.doubleBuffer)
                throw new IndexNotAvailableException("Context count is unavailable when double buffering because the target cycle is selected when the buffer is flushed");
            return isClosed ? -1 : StoreAppender.this.contextCount();
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
