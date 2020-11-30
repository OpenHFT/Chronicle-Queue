package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.batch.BatchAppender;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.queue.impl.table.AbstractTSQueueLock;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.BufferOverflowException;

import static net.openhft.chronicle.wire.Wires.*;

class StoreAppender extends AbstractCloseable
        implements ExcerptAppender, ExcerptContext, InternalAppender {
    @NotNull
    private final SingleChronicleQueue queue;
    @NotNull
    private final WriteLock writeLock;
    private final WriteLock appendLock;

    @NotNull
    private final StoreAppenderContext writeContext;
    private final WireStorePool storePool;
    private final boolean checkInterrupts;
    @Nullable
    SingleChronicleQueueStore store;
    private int cycle = Integer.MIN_VALUE;
    @Nullable
    private Wire wire;
    @Nullable
    private Wire wireForIndex;
    private long positionOfHeader = 0;
    private long lastIndex = Long.MIN_VALUE;
    private long lastPosition;
    private int lastCycle;
    @Nullable
    private Pretoucher pretoucher = null;
    private NativeBytesStore<Void> batchTmp;
    private Wire bufferWire = null;
    @UsedViaReflection
    private final Finalizer finalizer;
    private boolean disableThreadSafetyCheck;
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
        int cycle = queue.cycle();
        int lastCycle = queue.lastCycle();
        if (lastCycle != cycle && lastCycle >= 0)
            // ensure that the EOF is written on the last cycle
            setCycle2(lastCycle, false);
        finalizer = Jvm.isResourceTracing() ? new Finalizer() : null;
    }

    private void checkAppendLock() {
        checkAppendLock(false);
    }

    /**
     * check the appendLock
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
            throw new IllegalStateException("locked: unable to append because a lock is being held by pid=" + (myPID ? "me" : lockedBy));
        } else
            throw new IllegalStateException("locked: unable to append");
    }

    private static void releaseBytesFor(Wire w) {
        if (w != null) {
            w.bytes().releaseLast();
        }
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
    public void writeBytes(@NotNull final WriteBytesMarshallable marshallable) throws UnrecoverableTimeoutException {
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

    @Nullable
    @Override
    public Wire wire() {
        return wire;
    }

    @Override
    public long batchAppend(final int timeoutMS, final BatchAppender batchAppender) {
        throwExceptionIfClosed();

        long maxMsgSize = this.queue.blockSize() / 4;
        long startTime = System.currentTimeMillis();
        long count = 0;
        long lastIndex = -1;
        do {
            int defaultIndexSpacing = this.queue.rollCycle().defaultIndexSpacing();
            Wire wire = wire();
            int writeCount = Math.min(128 << 10,
                    (int) (defaultIndexSpacing - (lastIndex & (defaultIndexSpacing - 1)) - 1));

            if (wire != null && writeCount > 0) {
                MappedBytes bytes = (MappedBytes) wire.bytes();
                long address = bytes.addressForWrite(bytes.writePosition());
                long bstart = bytes.start();
                long bcap = bytes.realCapacity();
                long canWrite = bcap - (bytes.writePosition() - bstart);
                long lengthCount = batchAppender.writeMessages(address, canWrite, writeCount);
                bytes.writeSkip((int) lengthCount);
                lastIndex += lengthCount >> 32;
                count += lengthCount >> 32;

            } else {
                if (batchTmp == null) {
                    batchTmp = NativeBytesStore.lazyNativeBytesStoreWithFixedCapacity(maxMsgSize);
                }

                try (DocumentContext dc = writingDocument()) {
                    long lengthCount = batchAppender.writeMessages(batchTmp.addressForWrite(0), maxMsgSize, 1);
                    int len = (int) lengthCount;
                    dc.wire().bytes().write(batchTmp, (long) Integer.BYTES, len - Integer.BYTES);
                }
                lastIndex = lastIndexAppended();
                count++;
            }
        }
        while (startTime + timeoutMS > System.currentTimeMillis());

        return count;
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
        if (store.dataVersion() > 0)
            w.usePadding(true);
        return w;
    }

    /**
     * @return true if the header number is changed, otherwise false
     * @throws UnrecoverableTimeoutException
     */
    private boolean resetPosition() throws UnrecoverableTimeoutException {
        long originalHeaderNumber = wire.headerNumber();
        try {
            if (store == null || wire == null)
                return false;
            long position = store.writePosition();
            position(position, position);

            Bytes<?> bytes = wire.bytes();
            assert !SingleChronicleQueue.CHECK_INDEX || checkPositionOfHeader(bytes);

            final long headerNumber = store.lastSequenceNumber(this);
            wire.headerNumber(queue.rollCycle().toIndex(cycle, headerNumber + 1) - 1);

            assert !SingleChronicleQueue.CHECK_INDEX || wire.headerNumber() != -1 || checkIndex(wire.headerNumber(), positionOfHeader);

            bytes.writeLimit(bytes.capacity());

            assert !SingleChronicleQueue.CHECK_INDEX || checkWritePositionHeaderNumber();
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
        return isReadyData(header) || isNotComplete(header);
    }

    @NotNull
    @Override
    public DocumentContext writingDocument() throws UnrecoverableTimeoutException {
        return writingDocument(false); // avoid overhead of a default method.
    }

    @NotNull
    @Override
    public DocumentContext writingDocument(final boolean metaData) throws UnrecoverableTimeoutException {
        throwExceptionIfClosed();
        // we allow the sink process to write metaData
        checkAppendLock(metaData);
        count++;
        if (count > 1) {
            assert metaData == writeContext.metaData;
            return writeContext;
        }

        if (queue.doubleBuffer && writeLock.locked() && !metaData) {
            writeContext.isClosed = false;
            writeContext.rollbackOnClose = false;
            writeContext.buffered = true;
            if (bufferWire == null) {
                Bytes bufferBytes = Bytes.allocateElasticOnHeap();
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

            int safeLength = (int) queue.overlapSize();
            resetPosition();
            assert !SingleChronicleQueue.CHECK_INDEX || checkWritePositionHeaderNumber();

            // sets the writeLimit based on the safeLength
            openContext(metaData, safeLength);
        }
        // there is nothing to read.
        wire.bytes().readPosition(wire.bytes().writePosition());
        return writeContext;
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) {
        if (!CHECK_THREAD_SAFETY)
            this.threadSafetyCheck(true);
        if (wire != null && writeContext.isOpen() && writeContext.chainedElement())
            return writeContext;
        return writingDocument(metaData);
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
                    if (!store.writeEOF(wire, timeoutMS()))
                        break;
                }
                cur--;
            }
        }

        setCycle2(lastCycle, true);
    }

    private long writeHeader(@NotNull final Wire wire, final int safeLength) {
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

    private void openContext(final boolean metaData, final int safeLength) {
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
                ae.printStackTrace();
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
    public void writeBytes(@NotNull final BytesStore bytes) throws UnrecoverableTimeoutException {
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
     * Write bytes at an index, but only if the index is at the end of the chronicle. If index is after the end of the chronicle, throw an
     * IllegalStateException. If the index is before the end of the chronicle then do not change the state of the chronicle.
     * <p>Thread-safe</p>
     *
     * @param index index to write at. Only if index is at the end of the chronicle will the bytes get written
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
            setCycle2(cycle, true);
        else if (queue.rollCycle().toCycle(wire.headerNumber()) != cycle)
            rollCycleTo(cycle);

        long headerNumber = wire.headerNumber();
        boolean isNextIndex = index == headerNumber + 1;
        if (!isNextIndex) {

            // in case our cached headerNumber is incorrect.
            if (resetPosition()) {

                headerNumber = wire.headerNumber();

                /// if the header number has changed then we will have roll
                if (queue.rollCycle().toCycle(headerNumber) != cycle) {
                    rollCycleTo(cycle);
                    headerNumber = wire.headerNumber();
                }
            }

            isNextIndex = index == headerNumber + 1;
            if (!isNextIndex) {
                if (index > headerNumber + 1)
                    throw new IllegalStateException("Unable to move to index " + Long.toHexString(index) + " beyond the end of the queue, current: " + Long.toHexString(headerNumber));

                // this can happen when using queue replication when we are back filling from a number of sinks at them same time
                // its normal behaviour in the is use case so should not be a WARN
                if (Jvm.isDebugEnabled(getClass()))
                    Jvm.debug().on(getClass(), "Trying to overwrite index " + Long.toHexString(index) + " which is before the end of the queue");
                return;
            }
        }
        writeBytesInternal(bytes, metadata);

        headerNumber = wire.headerNumber();
        boolean isIndex = index == headerNumber;
        if (!isIndex) {
            writeBytesInternal(bytes, metadata);
            Thread.yield();
        }
    }

    private void writeBytesInternal(@NotNull final BytesStore bytes, boolean metadata) {
        assert writeLock.locked();
        try {
            int safeLength = (int) queue.overlapSize();
            assert count == 0 : "count=" + count;
            openContext(metadata, safeLength);

            try {
                writeContext.wire().bytes().write(bytes);
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
        position0(position, startOfMessage);
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
    private void rollCycleTo(final int cycle) throws UnrecoverableTimeoutException {

        // only a valid check if the wire was set.
        if (this.cycle == cycle)
            throw new AssertionError();

        store.writeEOF(wire, timeoutMS());

        int lastCycle = queue.lastCycle();

        if (lastCycle < cycle && lastCycle != this.cycle && lastCycle >= 0) {
            setCycle2(lastCycle, false);
            rollCycleTo(cycle);
        } else {
            setCycle2(cycle, true);
        }
    }

    /**
     * Write an EOF marker on the current cycle if it is about to roll. It would do this any way if a new message was written, but this doesn't create
     * a new cycle or add a message. Only used by tests.
     */
    void writeEndOfCycleIfRequired() {
        if (wire != null && queue.cycle() != cycle)
            store.writeEOF(wire, timeoutMS());
    }

    void writeIndexForPosition(final long index, final long position)
            throws UnrecoverableTimeoutException, StreamCorruptedException {

        long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
        store.setPositionForSequenceNumber(this, sequenceNumber, position);
    }

    boolean checkIndex(final long index, final long position) {
        try {
            final long seq1 = queue.rollCycle().toSequenceNumber(index + 1) - 1;
            final long seq2 = store.sequenceForPosition(this, position, true);

            if (seq1 != seq2) {
                final long seq3 = ((SingleChronicleQueueStore) store).indexing
                        .linearScanByPosition(wireForIndex(), position, 0, 0, true);
                System.out.println("Thread=" + Thread.currentThread().getName() +
                        " pos: " + position +
                        " seq1: " + Long.toHexString(seq1) +
                        " seq2: " + Long.toHexString(seq2) +
                        " seq3: " + Long.toHexString(seq3));

                System.out.println(store.dump());

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

    void position0(final long position, final long startOfMessage) {
        this.positionOfHeader = position;
        wire.bytes().writePosition(startOfMessage);
    }

    @Override
    public ExcerptAppender disableThreadSafetyCheck(boolean disableThreadSafetyCheck) {
        this.disableThreadSafetyCheck = disableThreadSafetyCheck;
        return this;
    }

    @Override
    protected boolean threadSafetyCheck(boolean isUsed) {
        return disableThreadSafetyCheck
                || super.threadSafetyCheck(isUsed);
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
                    doRollback(interrupted);
                    return;
                }

                if (wire == StoreAppender.this.wire) {

                    try {
                        wire.updateHeader(positionOfHeader, metaData, 0);
                    } catch (IllegalStateException e) {
                        if (queue.isClosed())
                            return;
                        throw e;
                    }

                    lastPosition = positionOfHeader;
                    lastCycle = cycle;

                    if (!metaData) {
                        lastIndex(wire.headerNumber());
                        store.writePosition(positionOfHeader);
                        if (lastIndex != Long.MIN_VALUE)
                            writeIndexForPosition(lastIndex, positionOfHeader);
                    }

                } else if (wire != null) {
                    if (buffered) {
                        writeBytes(wire.bytes());
                        unlock = false;
                    } else {
                        writeBytesInternal(wire.bytes(), metaData);
                        wire = StoreAppender.this.wire;
                    }
                }
            } catch (StreamCorruptedException | UnrecoverableTimeoutException | InterruptedException e) {
                throw new IllegalStateException(e);
            } finally {
                isClosed = true;
                if (unlock)
                    try {
                        writeLock.unlock();
                    } catch (Exception ex) {
                        Jvm.warn().on(getClass(), "Exception while unlocking: ", ex);
                    }
            }
        }

        private void doRollback(final boolean interrupted) {
            if (interrupted)
                Jvm.warn().on(getClass(), "Thread is interrupted. Can't guarantee complete message, so not committing");
            // zero out all contents...
            for (long i = positionOfHeader; i <= wire.bytes().writePosition(); i++)
                wire.bytes().writeByte(i, (byte) 0);
            long lastPosition = StoreAppender.this.lastPosition;
            position0(lastPosition, lastPosition);
            ((AbstractWire) wire).forceNotInsideHeader();
        }

        @Override
        public long index() throws IORuntimeException {
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
