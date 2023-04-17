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
import net.openhft.chronicle.queue.util.MicroTouched;
import net.openhft.chronicle.queue.util.PretouchUtil;
import net.openhft.chronicle.wire.*;
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
                        resetPosition(false);
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

    private static void releaseBytesFor(Wire w) {
        if (w != null) {
            w.bytes().release(INIT);
        }
    }

    private void checkAppendLock() {
        checkAppendLock(false);
    }

    /**
     * check the appendLock
     *
     * @param allowMyProcess this will only be true for any writes coming from the sink replicator
     */
    private void checkAppendLock(boolean allowMyProcess) {
        if (appendLock.locked())
            checkAppendLockLocked(allowMyProcess);
    }

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
        } else
            throw new IllegalStateException("locked: unable to append, file=" + queue.file());
    }

    /**
     * @param marshallable to write to excerpt.
     */
    @Override
    public void writeBytes(@NotNull final WriteBytesMarshallable marshallable) {
        throwExceptionIfClosed();

        try (DocumentContext dc = writingDocument()) {
            Bytes<?> bytes = dc.wire().bytes();
            long wp = bytes.writePosition();
            marshallable.writeMarshallable(bytes);
            if (wp == bytes.writePosition())
                dc.rollbackOnClose();
        }
    }

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
                pretoucher = PretouchUtil.createPretoucher(queue());

            pretoucher.execute();

        } catch (Throwable e) {
            Jvm.warn().on(getClass(), e);
            throw Jvm.rethrow(e);
        }
    }

    @Override
    public boolean microTouch() {
        throwExceptionIfClosed();

        if (microtoucher == null)
            microtoucher = new MicroToucher(this);

        return microtoucher.execute();
    }

    @Override
    public void bgMicroTouch() {
        if (isClosed())
            throw new ClosedIllegalStateException(getClass().getName() + " closed for " + Thread.currentThread().getName(), closedHere);

        if (microtoucher == null)
            microtoucher = new MicroToucher(this);

        microtoucher.bgExecute();
    }

    @Nullable
    @Override
    public Wire wire() {
        return wire;
    }

    @Nullable
    @Override
    public Wire wireForIndex() {
        return wireForIndex;
    }

    @Override
    public long timeoutMS() {
        return queue.timeoutMS;
    }

    void lastIndex(long index) {
        this.lastIndex = index;
    }

    @Override
    public boolean recordHistory() {
        return sourceId() != 0;
    }

    void setCycle(int cycle) {
        if (cycle != this.cycle)
            setCycle2(cycle, WireStoreSupplier.CreateStrategy.CREATE);
    }

    private void setCycle2(final int cycle, final WireStoreSupplier.CreateStrategy createStrategy) {
        queue.throwExceptionIfClosed();
        if (cycle < 0)
            throw new IllegalArgumentException("You can not have a cycle that starts " +
                    "before Epoch. cycle=" + cycle);

        SingleChronicleQueue queue = this.queue;

        SingleChronicleQueueStore oldStore = this.store;

        SingleChronicleQueueStore newStore = storePool.acquire(cycle, createStrategy, oldStore);

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
        resetPosition(false);
        queue.onRoll(cycle);
    }

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

    private Wire createWire(@NotNull final WireType wireType) {
        final Wire w = wireType.apply(store.bytes());
        w.usePadding(store.dataVersion() > 0);
        return w;
    }

    /**
     * @return true if the header number is changed, otherwise false
     * @throws UnrecoverableTimeoutException todo
     */
    private boolean resetPosition(boolean exact) {
        long originalHeaderNumber = wire.headerNumber();
        try {
            if (store == null || wire == null)
                return false;
            long position = store.writePosition();
            position(position, position);

            Bytes<?> bytes = wire.bytes();
            assert !QueueSystemProperties.CHECK_INDEX || checkPositionOfHeader(bytes);

            final long lastSequenceNumber = exact ? store.exactLastSequenceNumber(this)
                    : store.approximateLastSequenceNumber(this);
            wire.headerNumber(queue.rollCycle().toIndex(cycle, lastSequenceNumber + 1) - 1);

            assert !QueueSystemProperties.CHECK_INDEX || wire.headerNumber() != -1 || checkIndex(wire.headerNumber(), positionOfHeader);

            bytes.writeLimit(bytes.capacity());

            assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();
            return originalHeaderNumber != wire.headerNumber();

        } catch (@NotNull BufferOverflowException | StreamCorruptedException e) {
            throw new AssertionError(e);
        }
    }

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

    private StoreAppender.StoreAppenderContext prepareAndReturnWriteContext(boolean metaData) {
        if (count > 1) {
            assert metaData == context.metaData;
            return context;
        }

        if (queue.doubleBuffer && writeLock.locked() && !metaData) {
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
                resetPosition(false);
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

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) {
        if (!DISABLE_THREAD_SAFETY)
            this.threadSafetyCheck(true);
        if (context.wire != null && context.isOpen() && context.chainedElement())
            return context;
        return writingDocument(metaData);
    }

    /**
     * Ensure any missing EOF markers are added back to previous cycles
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

    private void setWireIfNull(final int cycle) {
        normaliseEOFs0(cycle);

        setCycle2(cycle, WireStoreSupplier.CreateStrategy.CREATE);
    }

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

    private void openContext(final boolean metaData, final long safeLength) {
        assert wire != null;
        this.positionOfHeader = writeHeader(wire, safeLength); // sets wire.bytes().writePosition = position + 4;
        context.isClosed = false;
        context.rollbackOnClose = false;
        context.buffered = false;
        context.wire = wire; // Jvm.isDebug() ? acquireBufferWire() : wire;
        context.metaData(metaData);
    }

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
                AssertionError ae = new AssertionError(message);
                throw ae;
            }
        } catch (Exception e) {
            // TODO FIX
            Jvm.warn().on(getClass(), e);
            throw Jvm.rethrow(e);
        }
        return true;
    }

    @Override
    public int sourceId() {
        return queue.sourceId;
    }

    @Override
    public void writeBytes(@NotNull final BytesStore bytes) {
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

            assert ((AbstractWire) wire).isInsideHeader();
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
     * Write bytes at an index, but only if the index is at the end of the queue (*or* end of cycle).
     * If index is after the end of the queue (or cycle), throw an IllegalStateException.
     * If the index is before the end of the queue then do not overwrite the contents of the queue.
     * <p>If the index is at the end of a cycle (but not the queue) this will overwrite the EOF marker
     * of that cycle. It is the caller's responsibility to call {@link #normaliseEOFs()} after.
     * <p>Users are advised that the behaviour of this method may change in the future
     * <p>Thread-safe
     *
     * @param index index to write at. Only if index is at the end of the queue (or cycle) will the bytes get written
     * @param bytes payload
     */
    public void writeBytes(final long index, @NotNull final BytesStore bytes) {
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
     */
    protected void writeBytesInternal(final long index, @NotNull final BytesStore bytes) {
        writeBytesInternal(index, bytes, false);
    }

    protected void writeBytesInternal(final long index, @NotNull final BytesStore bytes, boolean metadata) {
        checkAppendLock(true);

        final int cycle = queue.rollCycle().toCycle(index);

        if (wire == null)
            setWireIfNull(cycle);

        /// if the header number has changed then we will have roll
        if (this.cycle != cycle)
            rollCycleTo(cycle, this.cycle > cycle);

        // in case our cached headerNumber is incorrect.
        resetPosition(true);

        long headerNumber = wire.headerNumber();

        boolean isNextIndex = index == headerNumber + 1;
        if (!isNextIndex) {
            if (index > headerNumber + 1)
                throw new IllegalStateException("Unable to move to index " + Long.toHexString(index) + " beyond the end of the queue, current: " + Long.toHexString(headerNumber));

            // this can happen when using queue replication when we are back filling from a number of sinks at them same time
            // its normal behaviour in the is use case so should not be a WARN
            if (Jvm.isDebugEnabled(getClass()))
                Jvm.debug().on(getClass(), "Trying to overwrite index " + Long.toHexString(index) + " which is before the end of the queue");
            return;
        }

        writeBytesInternal(bytes, metadata);
        //assert !QueueSystemProperties.CHECK_INDEX || checkWritePositionHeaderNumber();

        headerNumber = wire.headerNumber();
        boolean isIndex = index == headerNumber;
        if (!isIndex) {
            throw new IllegalStateException("index: " + index + ", header: " + headerNumber);
        }
    }

    private void writeBytesInternal(@NotNull final BytesStore bytes, boolean metadata) {
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

    /*
     * wire must be not null when this method is called
     */
    // throws UnrecoverableTimeoutException

    private void rollCycleTo(final int toCycle) {
        rollCycleTo(toCycle, this.cycle > toCycle);
    }

    private void rollCycleTo(final int cycle, boolean suppressEOF) {

        // only a valid check if the wire was set.
        if (this.cycle == cycle)
            throw new AssertionError();

        if (!suppressEOF) {
            assert queue.writeLock().locked();
            store.writeEOF(wire, timeoutMS());
        }

        int lastExistingCycle = queue.lastCycle();

        if (lastExistingCycle < cycle && lastExistingCycle != this.cycle && lastExistingCycle >= 0) {
            setCycle2(lastExistingCycle, WireStoreSupplier.CreateStrategy.READ_ONLY);
            rollCycleTo(cycle);
        } else {
            setCycle2(cycle, WireStoreSupplier.CreateStrategy.CREATE);
        }
    }

    // throws UnrecoverableTimeoutException
    void writeIndexForPosition(final long index, final long position) throws StreamCorruptedException {
        long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
        store.setPositionForSequenceNumber(this, sequenceNumber, position);
    }

    boolean checkIndex(final long index, final long position) {
        try {
            final long seq1 = queue.rollCycle().toSequenceNumber(index + 1) - 1;
            final long seq2 = store.sequenceForPosition(this, position, true);

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

    void position0(final long position, final long startOfMessage, Bytes<?> bytes) {
        this.positionOfHeader = position;
        bytes.writeLimit(bytes.capacity());
        bytes.writePosition(startOfMessage);
    }

    @Override
    public @NotNull StoreAppender disableThreadSafetyCheck(boolean disableThreadSafetyCheck) {
        super.singleThreadedCheckDisabled(disableThreadSafetyCheck);
        return this;
    }

    @Override
    public File currentFile() {
        SingleChronicleQueueStore store = this.store;
        return store == null ? null : store.currentFile();
    }

    @Override
    public void sync() {
        if (store == null)
            return;

        final Bytes<?> bytes = context.wire().bytes();
        if (bytes.bytesStore() instanceof MappedBytesStore) {
            MappedBytesStore mbs = (MappedBytesStore) bytes.bytesStore();
            mbs.syncUpTo(bytes.readPosition());
            queue.lastIndexMSynced(lastIndex);
        }
    }

    private class Finalizer {
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            context.rollbackOnClose();
            warnAndCloseIfNotClosed();
        }
    }

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

        @Override
        public void reset() {
            isClosed = true;
            metaData = false;
            rollbackOnClose = false;
            buffered = false;
            alreadyClosedFound = false;
            chainedElement = false;
        }

        @Override
        public int sourceId() {
            return StoreAppender.this.sourceId();
        }

        @Override
        public boolean isPresent() {
            return false;
        }

        @Override
        public Wire wire() {
            return wire;
        }

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

        @Override
        public void close() {
            close(true);
        }

        public void close(boolean unlock) {
            if (chainedElement)
                return;
            if (isClosed) {
                Jvm.warn().on(getClass(), "Already Closed, close was called twice.", new StackTrace("Second close", closedHere));
                alreadyClosedFound = true;
                return;
            }
            count--;
            if (count > 0)
                return;

            if (alreadyClosedFound) {
                closedHere = new StackTrace("Closed here");
            }

            try {
                // historically there have been problems with an interrupted thread causing exceptions
                // in calls below, and we saw half-written messages
                final boolean interrupted = checkInterrupts && Thread.currentThread().isInterrupted();
                if (interrupted)
                    throw new InterruptedException();
                if (rollbackOnClose) {
                    doRollback();
                    return;
                }

                if (wire == StoreAppender.this.wire) {
//                    final BytesStore bs = wire.bytes().bytesStore();
                    try {
                        wire.updateHeader(positionOfHeader, metaData, 0);
                    } catch (IllegalStateException e) {
                        if (queue.isClosed())
                            return;
                        throw e;
                    }
//                    if (bs != wire.bytes().bytesStore())
//                        throw new AssertionError("header had to rewind to be written");

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
                wire.bytes().writePositionForHeader(true);
                isClosed = true;
                if (unlock)
                    try {
                        writeLock.unlock();
                    } catch (Exception ex) {
                        Jvm.warn().on(getClass(), "Exception while unlocking: ", ex);
                    }
            }
        }

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
                    ((AbstractWire) wire).forceNotInsideHeader();
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
        public long index() {
            if (buffered) {
                throw new IndexNotAvailableException("Index is unavailable when double buffering");
            }
            if (this.wire.headerNumber() == Long.MIN_VALUE) {
                try {
                    wire.headerNumber(queue.rollCycle().toIndex(cycle, store.lastSequenceNumber(StoreAppender.this)));
                    long headerNumber0 = wire.headerNumber();
                    assert (((AbstractWire) this.wire).isInsideHeader());
                    return isMetaData() ? headerNumber0 : headerNumber0 + 1;
                } catch (IOException e) {
                    throw new IORuntimeException(e);
                }
            }

            return isMetaData() ? Long.MIN_VALUE : this.wire.headerNumber() + 1;
        }

        @Override
        public boolean isOpen() {
            return !isClosed;
        }

        @Override
        public boolean isNotComplete() {
            return !isClosed;
        }

        @Override
        public void start(boolean metaData) {
            throw new UnsupportedOperationException();
        }

        public void metaData(boolean metaData) {
            this.metaData = metaData;
        }

        @Override
        public boolean chainedElement() {
            return chainedElement;
        }

        @Override
        public void chainedElement(boolean chainedElement) {
            this.chainedElement = chainedElement;
        }
    }
}
