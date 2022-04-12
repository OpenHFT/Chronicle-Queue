package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueSystemProperties;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.queue.impl.table.AbstractTSQueueLock;
import net.openhft.chronicle.queue.util.MicroTouched;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.BufferOverflowException;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueue.WARN_SLOW_APPENDER_MS;
import static net.openhft.chronicle.wire.Wires.*;

class StoreAppender extends AbstractCloseable
        implements ExcerptAppender, ExcerptContext, InternalAppender, MicroTouched {
    @NotNull
    private final SingleChronicleQueue queue;
    @NotNull
    private final WriteLock writeLock;
    private final WriteLock appendLock;

    @NotNull
    private final StoreAppenderContext writeContext;
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
    private int lastCycle;
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

        this.writeContext = new StoreAppenderContext();

        // always put references to "this" last.
        queue.addCloseListener(this);

        queue.cleanupStoreFilesWithNoData();
        normaliseEOFs();

        int cycle = queue.cycle();
        int lastCycle = queue.lastCycle();
        if (lastCycle != cycle && lastCycle >= 0) {
            final WriteLock writeLock = queue.writeLock();
            writeLock.lock();
            try {
                // ensure that the EOF is written on the last cycle
                setCycle2(lastCycle, false);
            } finally {
                writeLock.unlock();
            }
        }
        finalizer = Jvm.isResourceTracing() ? new Finalizer() : null;
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

    @Deprecated // Should not be providing accessors to reference-counted objects
    @NotNull
    WireStore store() {
        if (store == null)
            setCycle(cycle());
        return store;
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
                pretoucher = new Pretoucher(queue());

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
            setCycle2(cycle, true);
    }

    private void setCycle2(final int cycle, final boolean createIfAbsent) {
        queue.throwExceptionIfClosed();
        if (cycle < 0)
            throw new IllegalArgumentException("You can not have a cycle that starts " +
                    "before Epoch. cycle=" + cycle);

        SingleChronicleQueue queue = this.queue;

        SingleChronicleQueueStore oldStore = this.store;

        SingleChronicleQueueStore newStore = storePool.acquire(cycle, queue.epoch(), createIfAbsent, oldStore);

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

        assert wire.startUse();
        wire.parent(this);
        wire.pauser(queue.pauserSupplier.get());
        resetPosition();
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
            assert wire != old || wire == null;
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
    private boolean resetPosition() {
        long originalHeaderNumber = wire.headerNumber();
        try {
            if (store == null || wire == null)
                return false;
            long position = store.writePosition();
            position(position, position);

            Bytes<?> bytes = wire.bytes();
            assert !QueueSystemProperties.CHECK_INDEX || checkPositionOfHeader(bytes);

            final long lastSequenceNumber = store.lastSequenceNumber(this);
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
            assert metaData == writeContext.metaData;
            return writeContext;
        }

        if (queue.doubleBuffer && writeLock.locked() && !metaData) {
            writeContext.isClosed = false;
            writeContext.rollbackOnClose = false;
            writeContext.buffered = true;
            if (bufferWire == null) {
                Bytes<?> bufferBytes = Bytes.allocateElasticOnHeap();
                bufferWire = queue().wireType().apply(bufferBytes);
            }
            writeContext.wire = bufferWire;
            writeContext.metaData(false);
        } else {
            writeLock.lock();
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
        }

        return writeContext;
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) {
        if (!DISABLE_THREAD_SAFETY)
            this.threadSafetyCheck(true);
        if (writeContext.wire != null && writeContext.isOpen() && writeContext.chainedElement())
            return writeContext;
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
            normaliseEOFs0();
        } finally {
            writeLock.unlock();
            long tookMillis = (System.nanoTime() - start) / 1_000_000;
            if (tookMillis > WARN_SLOW_APPENDER_MS)
                Jvm.perf().on(getClass(), "Took " + tookMillis + "ms to normaliseEOFs");
        }
    }

    private void normaliseEOFs0() {
        int last = queue.lastCycle();
        int first = queue.firstCycle();

        for (int cycle = first; cycle < last; ++cycle) {
            setCycle2(cycle, false);
            if (wire != null) {
                assert queue.writeLock().locked();
                store.writeEOF(wire, timeoutMS());
            }
        }
    }

    private void setWireIfNull(final int cycle) {
        int lastCycle = queue.lastCycle();
        if (lastCycle == Integer.MIN_VALUE)
            lastCycle = cycle;
        else {
            int cur = lastCycle - 1;
            int firstCycle = queue.firstCycle();
            while (cur >= firstCycle) {
                setCycle2(cur, false);
                if (wire != null) {
                    assert queue.writeLock().locked();
                    if (!store.writeEOF(wire, timeoutMS()))
                        break;
                }
                cur--;
            }
        }

        setCycle2(lastCycle, true);
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
        writeContext.isClosed = false;
        writeContext.rollbackOnClose = false;
        writeContext.buffered = false;
        writeContext.wire = wire; // Jvm.isDebug() ? acquireBufferWire() : wire;
        writeContext.metaData(metaData);
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
            lastCycle = cycle;
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

        // in case our cached headerNumber is incorrect.
        resetPosition();

        /// if the header number has changed then we will have roll
        if (this.cycle != cycle)
            rollCycleTo(cycle, this.cycle > cycle);

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
                final Bytes<?> bytes0 = writeContext.wire().bytes();
                bytes0.readPosition(bytes0.writePosition());
                bytes0.write(bytes);
            } finally {
                writeContext.close(false);
                count = 0;
            }
        } finally {
            writeContext.isClosed = true;
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
            long index = queue.rollCycle().toIndex(lastCycle, sequenceNumber);
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

    private void rollCycleTo(final int cycle) {
        rollCycleTo(cycle, false);
    }

    private void rollCycleTo(final int cycle, boolean suppressEOF) {

        // only a valid check if the wire was set.
        if (this.cycle == cycle)
            throw new AssertionError();

        if (!suppressEOF) {
            assert queue.writeLock().locked();
            store.writeEOF(wire, timeoutMS());
        }

        int lastCycle = queue.lastCycle();

        if (lastCycle < cycle && lastCycle != this.cycle && lastCycle >= 0) {
            setCycle2(lastCycle, false);
            rollCycleTo(cycle);
        } else {
            setCycle2(cycle, true);
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
                ", lastCycle=" + lastCycle +
                '}';
    }

    void position0(final long position, final long startOfMessage, Bytes<?> bytes) {
        this.positionOfHeader = position;
        bytes.writeLimit(bytes.capacity());
        bytes.writePosition(startOfMessage);
    }

    @Override
    public @NotNull StoreAppender disableThreadSafetyCheck(boolean disableThreadSafetyCheck) {
        super.disableThreadSafetyCheck(disableThreadSafetyCheck);
        return this;
    }

    @Override
    public File currentFile() {
        SingleChronicleQueueStore store = this.store;
        return store == null ? null : store.currentFile();
    }

    private class Finalizer {
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            writeContext.rollbackOnClose();
            warnAndCloseIfNotClosed();
        }
    }

    @Override
    public void clearUsedByThread() {
        throw new UnsupportedOperationException("clearUsedByThread should never be called on a StoreAppender. They are ThreadLocal and bound to the thread they're created on.");
    }

    @Override
    public void resetUsedByThread() {
        throw new UnsupportedOperationException("resetUsedByThread should never be called on a StoreAppender. They are ThreadLocal and bound to the thread they're created on.");
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
                    lastCycle = cycle;

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
