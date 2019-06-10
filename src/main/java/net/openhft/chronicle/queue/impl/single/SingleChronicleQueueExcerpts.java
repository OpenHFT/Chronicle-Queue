/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.bytes.util.DecoratedBufferUnderflowException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.UnsafeMemory;
import net.openhft.chronicle.core.annotation.PackageLocal;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.pool.StringBuilderPool;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.batch.BatchAppender;
import net.openhft.chronicle.queue.impl.*;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.BufferOverflowException;
import java.text.ParseException;

import static net.openhft.chronicle.queue.TailerDirection.*;
import static net.openhft.chronicle.queue.TailerState.*;
import static net.openhft.chronicle.queue.impl.single.ScanResult.*;
import static net.openhft.chronicle.wire.BinaryWireCode.FIELD_NUMBER;
import static net.openhft.chronicle.wire.Wires.*;

public class SingleChronicleQueueExcerpts {

    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueueExcerpts.class);
    private static final int MESSAGE_HISTORY_METHOD_ID = -1;
    private static StringBuilderPool SBP = new StringBuilderPool();

    private static void releaseWireResources(final Wire wire) {
        StoreComponentReferenceHandler.queueForRelease(wire);
    }

    // *************************************************************************
    //
    // APPENDERS
    //
    // *************************************************************************

    public interface InternalAppender {
        void writeBytes(long index, BytesStore bytes);
    }

    static class StoreAppender implements ExcerptAppender, ExcerptContext, InternalAppender {

        @NotNull
        private final SingleChronicleQueue queue;
        @NotNull
        private final WriteLock writeLock;
        @NotNull
        private final StoreAppenderContext context;
        private final ClosableResources<?> closableResources;
        private final WireStorePool storePool;
        private final boolean checkInterrupts;
        @Nullable
        WireStore store;
        private int cycle = Integer.MIN_VALUE;
        @Nullable
        private Wire wire;
        @Nullable
        private Wire wireForIndex;
        private long position = 0;
        private long lastIndex = Long.MIN_VALUE;
        private long lastPosition;
        private int lastCycle;
        @Nullable
        private Pretoucher pretoucher = null;
        private Padding padToCacheLines = Jvm.isArm() ? Padding.WORD : Padding.SMART;
        private NativeBytesStore<Void> batchTmp;

        StoreAppender(@NotNull SingleChronicleQueue queue,
                      @NotNull WireStorePool storePool,
                      boolean checkInterrupts) {
            this.queue = queue;
            this.storePool = storePool;
            this.checkInterrupts = checkInterrupts;

            this.writeLock = queue.writeLock();
            this.context = new StoreAppenderContext();
            this.closableResources = new ClosableResources<>(storePool);

            // always put references to "this" last.
            queue.addCloseListener(this, StoreAppender::close);
        }

        @Deprecated // Should not be providing accessors to reference-counted objects
        @NotNull
        WireStore store() {
            if (store == null)
                setCycle(cycle());
            return store;
        }

        @Override
        @NotNull
        public Padding padToCacheAlignMode() {
            return padToCacheLines;
        }

        /**
         * @param padToCacheLines the default for chronicle queue is Padding.SMART, which automatically pads all method calls other than {@link
         *                        StoreAppender#writeBytes(net.openhft.chronicle.bytes.WriteBytesMarshallable)} and   {@link
         *                        StoreAppender#writeText(java.lang.CharSequence)}. Which can not be padded with out changing the message format, The
         *                        reason we pad is to ensure that a message header does not straggle a cache line.
         */
        @Override
        public void padToCacheAlign(Padding padToCacheLines) {
            this.padToCacheLines = padToCacheLines;
        }

        /**
         * @param marshallable to write to excerpt.
         */
        @Override
        public void writeBytes(@NotNull WriteBytesMarshallable marshallable) throws UnrecoverableTimeoutException {
            try (DocumentContext dc = writingDocument()) {
                Bytes<?> bytes = dc.wire().bytes();
                long wp = bytes.writePosition();
                marshallable.writeMarshallable(bytes);
                if (wp == bytes.writePosition())
                    dc.rollbackOnClose();
                else if (padToCacheAlignMode() == Padding.WORD)
                    ((StoreAppenderContext) dc).padToWordAlign = true;
                else if (padToCacheAlignMode() != Padding.CACHE_LINE)
                    ((StoreAppenderContext) dc).padToCacheAlign = false;
            }
        }

        void close() {
            // todo remove
            Jvm.warn().on(getClass(), "Closing store append for " + queue.file().getAbsolutePath());
            Wire w0 = wireForIndex;
            wireForIndex = null;
            if (w0 != null)
                w0.bytes().release();
            Wire w = wire;
            wire = null;
            if (w != null) {
                w.bytes().release();
            }

            if (pretoucher != null)
                pretoucher.close();

            if (store != null) {
                storePool.release(store);
            }
            store = null;
            storePool.close();
        }

        /**
         * pretouch() has to be run on the same thread, as the thread that created the appender. If you want to use pretouch() in another thread, you
         * must first create or have an appender that was created on this thread, and then use this appender to call the pretouch()
         */
        @Override
        public void pretouch() {
            if (queue.isClosed())
                throw new RuntimeException("Queue Closed");
            try {
                if (pretoucher == null)
                    pretoucher = new Pretoucher(queue());

                pretoucher.execute();
            } catch (Throwable e) {
                Jvm.warn().on(getClass(), e);
                Jvm.rethrow(e);
            }
        }

        @Nullable
        @Override
        public Wire wire() {
            return wire;
        }

        @Override
        public long batchAppend(final int timeoutMS, BatchAppender batchAppender) {

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

        private void setCycle2(int cycle, boolean createIfAbsent) {
            if (cycle < 0)
                throw new IllegalArgumentException("You can not have a cycle that starts " +
                        "before Epoch. cycle=" + cycle);

            SingleChronicleQueue queue = this.queue;

            WireStore store = this.store;
            if (store != null) {
                storePool.release(store);
            }

            this.store = storePool.acquire(cycle, queue.epoch(), createIfAbsent);
            closableResources.storeReference = this.store;
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

        private void resetWires(@NotNull ChronicleQueue queue) {
            WireType wireType = queue.wireType();
            {
                Wire oldw = this.wire;
                this.wire = store == null ? null : wireType.apply(store.bytes());
                closableResources.wireReference = this.wire == null ? null : this.wire.bytes();
                assert wire != oldw || wire == null;
                if (oldw != null) {
                    releaseWireResources(oldw);
                }
            }
            {
                Wire old = this.wireForIndex;
                this.wireForIndex = store == null ? null : wireType.apply(store.bytes());
                closableResources.wireForIndexReference = this.wireForIndex == null ? null : wireForIndex.bytes();
                assert wire != old || wire == null;
                if (old != null) {
                    releaseWireResources(old);
                }
            }

        }

        private void resetPosition() throws UnrecoverableTimeoutException {
            try {
                if (store == null || wire == null)
                    return;
                position(store.writePosition());

                assert position == 0 || Wires.isReadyData(wire.bytes().readVolatileInt(position));

                final long headerNumber = store.lastSequenceNumber(this);
                wire.headerNumber(queue.rollCycle().toIndex(cycle, headerNumber + 1) - 1);
                assert wire.headerNumber() != -1 || checkIndex(wire.headerNumber(), position);

            } catch (@NotNull BufferOverflowException | StreamCorruptedException e) {
                throw new AssertionError(e);
            }
            assert checkWritePositionHeaderNumber();
        }

        @NotNull
        @Override
        public DocumentContext writingDocument() throws UnrecoverableTimeoutException {
            return writingDocument(false); // avoid overhead of a default method.
        }

        @NotNull
        @Override
        public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
            if (queue.isClosed.get())
                throw new IllegalStateException("Queue is closed");
            writeLock.lock();
            assert checkWritePositionHeaderNumber();
            int cycle = queue.cycle();

            if (wire == null)
                setWireIfNull(cycle);

            if (this.cycle != cycle)
                rollCycleTo(cycle);

            int safeLength = (int) queue.overlapSize();

            openContext(metaData, safeLength);
            return context;
        }

        private void setWireIfNull(int cycle) {
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

        private long writeHeader(@NotNull Wire wire, int safeLength) {
            Bytes<?> bytes = wire.bytes();
            // writePosition points at the last record in the queue, so we can just skip it and we're ready for write
            long pos = position;
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

        private void openContext(boolean metaData, int safeLength) {
            assert wire != null;
            position(writeHeader(wire, safeLength));
            context.isClosed = false;
            context.rollbackOnClose = false;
            context.wire = wire; // Jvm.isDebug() ? acquireBufferWire() : wire;
            context.padToWordAlign = padToCacheAlignMode() == Padding.WORD;
            context.padToCacheAlign = padToCacheAlignMode() != Padding.NEVER;
            context.metaData(metaData);
        }

        boolean checkWritePositionHeaderNumber() {
            if (wire == null || wire.headerNumber() == Long.MIN_VALUE) return true;
            try {
                long pos = position;

                long seq1 = queue.rollCycle().toSequenceNumber(wire.headerNumber() + 1) - 1;
                long seq2 = store.sequenceForPosition(this, pos, true);

                if (seq1 != seq2) {
//                    System.out.println(queue.dump());
                    String message = "~~~~~~~~~~~~~~ " +
                            "thread: " + Thread.currentThread().getName() +
                            " pos: " + pos +
                            " header: " + wire.headerNumber() +
                            " seq1: " + seq1 +
                            " seq2: " + seq2;
                    //System.err.println(message);
                    new AssertionError(message).printStackTrace();
                    throw new AssertionError(message);
                }

            } catch (Exception e) {
                Jvm.fatal().on(getClass(), e);
                throw Jvm.rethrow(e);
            }
            return true;
        }

        @Override
        public int sourceId() {
            return queue.sourceId;
        }

        @Override
        public void writeBytes(@NotNull BytesStore bytes) throws UnrecoverableTimeoutException {
            writeLock.lock();
            try {
                int cycle = queue.cycle();
                if (wire == null)
                    setWireIfNull(cycle);

                if (this.cycle != cycle)
                    rollCycleTo(cycle);

                position(writeHeader(wire, (int) queue.overlapSize()));
                assert ((AbstractWire) wire).isInsideHeader();
                beforeAppend(wire, wire.headerNumber() + 1);
                Bytes<?> wireBytes = wire.bytes();
                wireBytes.write(bytes);
                if (padToCacheLines == Padding.WORD)
                    wireBytes.writeSkip((-wireBytes.writePosition()) & 0x3);
                wire.updateHeader(position, false, 0);
                lastIndex(wire.headerNumber());
                lastPosition = position;
                lastCycle = cycle;
                store.writePosition(position);
                writeIndexForPosition(lastIndex, position);
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
        public void writeBytes(long index, @NotNull BytesStore bytes) {

            if (queue.isClosed.get())
                throw new IllegalStateException("Queue is closed");

            writeLock.lock();
            try {
                int cycle = queue.rollCycle().toCycle(index);

                if (wire == null) {
                    setCycle2(cycle, true);
                } else if (this.cycle < cycle)
                    rollCycleTo(cycle);

                boolean rollbackDontClose = index != wire.headerNumber() + 1;
                if (rollbackDontClose) {
                    if (index > wire.headerNumber() + 1)
                        throw new IllegalStateException("Unable to move to index " + Long.toHexString(index) + " beyond the end of the queue");
                    // TODO: assert bytes.equalBytes(wire.bytes() ...);
                    Jvm.warn().on(getClass(), "Trying to overwrite index " + Long.toHexString(index) + " which is before the end of the queue");
                    return;
                }

                writeBytesInternal(index, bytes);
            } finally {
                writeLock.unlock();
            }
        }

        private void writeBytesInternal(long index, @NotNull BytesStore bytes) {
            assert writeLock.locked();
            try {
                if (wire == null) {
                    int cycle = queue.rollCycle().toCycle(index);
                    setCycle2(cycle, true);
                }
                int safeLength = (int) queue.overlapSize();
                openContext(false, safeLength);

                try {
                    context.wire().bytes().write(bytes);
                } finally {
                    context.close(false);
                }

            } finally {
                context.isClosed = true;
            }
        }

        private void position(long position) {
            // did the position jump too far forward.
            if (position > store.writePosition() + queue.blockSize())
                throw new IllegalArgumentException("pos: " + position + ", store.writePosition()=" +
                        store.writePosition() + " queue.blockSize()=" + queue.blockSize());
            // System.err.println("----- "+Thread.currentThread().getName()+" pos: "+position);
            this.position = position;
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

        @Override
        public Runnable getCloserJob() {
            return closableResources::releaseResources;
        }

        /*
         * overwritten in delta wire
         */
        void beforeAppend(Wire wire, long index) {
        }

        /*
         * wire must be not null when this method is called
         */
        private void rollCycleTo(int cycle) throws UnrecoverableTimeoutException {

            // only a valid check if the wire was set.
            if (this.cycle == cycle)
                throw new AssertionError();

            store.writeEOF(wire, timeoutMS());

            int lastCycle = queue.lastCycle;

            if (lastCycle != cycle && lastCycle > this.cycle) {
                setCycle2(lastCycle, false);
                rollCycleTo(cycle);
            } else {
                setCycle2(cycle, true);
            }
        }

        /**
         * Write an EOF marker on the current cycle if it is about to roll. It would do this any way if a new message was written, but this doesn't
         * create a new cycle or add a message. Only used by tests.
         */
        void writeEndOfCycleIfRequired() {
            if (wire != null && queue.cycle() != cycle)
                store.writeEOF(wire, timeoutMS());
        }

        void writeIndexForPosition(long index, long position)
                throws UnrecoverableTimeoutException, StreamCorruptedException {

            long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
            store.setPositionForSequenceNumber(this, sequenceNumber, position);
        }

        boolean checkIndex(long index, long position) {
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
                    ", position=" + position +
                    ", lastIndex=" + lastIndex +
                    ", lastPosition=" + lastPosition +
                    ", lastCycle=" + lastCycle +
                    '}';
        }

        class StoreAppenderContext implements DocumentContext {

            boolean isClosed;
            boolean padToWordAlign = false;
            boolean padToCacheAlign = true;
            private boolean metaData = false;
            private boolean rollbackOnClose = false;
            @Nullable
            private Wire wire;

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

            @Override
            public void metaData(boolean metaData) {
                this.metaData = metaData;
            }

            @Override
            public boolean isClosed() {
                return isClosed;
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

                if (isClosed) {
                    LOG.warn("Already Closed, close was called twice.");
                    return;
                }

                try {
                    final boolean interrupted = checkInterrupts && Thread.currentThread().isInterrupted();
                    if (rollbackOnClose || interrupted) {
                        doRollback(interrupted);
                        return;
                    }

                    if (wire == StoreAppender.this.wire) {
                        if (padToWordAlign)
                            wire.writeAlignTo(Integer.BYTES, 0);
                        else if (padToCacheAlign)
                            wire.padToCacheAlign();

                        try {
                            wire.updateHeader(position, metaData, 0);
                        } catch (IllegalStateException e) {
                            if (queue.isClosed())
                                return;
                            throw e;
                        }

                        lastPosition = position;
                        lastCycle = cycle;

                        if (!metaData) {
                            lastIndex(wire.headerNumber());
                            store.writePosition(position);
                            if (lastIndex != Long.MIN_VALUE)
                                writeIndexForPosition(lastIndex, position);
                            else
                                assert lastIndex == Long.MIN_VALUE || checkIndex(lastIndex, position);
                        }
                        assert checkWritePositionHeaderNumber();
                    } else if (wire != null) {
                        isClosed = true;
                        writeBytesInternal(wire.headerNumber(), wire.bytes());
                        wire = StoreAppender.this.wire;
                    }
                } catch (@NotNull StreamCorruptedException | UnrecoverableTimeoutException e) {
                    throw new IllegalStateException(e);
                } finally {
                    if (unlock)
                        try {
                            writeLock.unlock();
                        } catch (Exception ex) {
                            Jvm.warn().on(getClass(), "Exception while unlocking: ", ex);
                        }
                }
            }

            private void doRollback(boolean interrupted) {
                if (interrupted)
                    LOG.warn("Thread is interrupted. Can't guarantee complete message, so not committing");
                // zero out all contents...
                for (long i = position; i <= wire.bytes().writePosition(); i++)
                    wire.bytes().writeByte(i, (byte) 0);
                position = lastPosition;
                wire.bytes().writePosition(position);
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
            public boolean isNotComplete() {
                throw new UnsupportedOperationException();
            }
        }
    }

// *************************************************************************
//
// TAILERS
//
// *************************************************************************

    static final class ClosableResources<T extends StoreReleasable> {
        @NotNull
        private final T storeReleasable;
        private volatile Bytes wireReference = null;
        private volatile Bytes bufferWireReference = null;
        private volatile Bytes wireForIndexReference = null;
        volatile CommonStore storeReference = null;

        ClosableResources(@NotNull final T storeReleasable) {
            this.storeReleasable = storeReleasable;
        }

        private static void releaseIfNotNull(final Bytes bytesReference) {
            // Object is no longer reachable, check that it has not already been released
            if (bytesReference != null && bytesReference.refCount() > 0) {
                bytesReference.release();
            }
        }

        private void releaseResources() {
            releaseIfNotNull(wireForIndexReference);
            releaseIfNotNull(wireReference);
            releaseIfNotNull(bufferWireReference);

            // Object is no longer reachable, check that it has not already been released
            if (storeReference != null && storeReference.refCount() > 0) {
                storeReleasable.release(storeReference);
            }
        }
    }

    /**
     * Tailer
     */
    public static class StoreTailer implements ExcerptTailer, SourceContext, ExcerptContext {
        static final int INDEXING_LINEAR_SCAN_THRESHOLD = 70;
        @NotNull
        private final SingleChronicleQueue queue;
        private final LongValue indexValue;
        private final StoreTailerContext context = new StoreTailerContext();
        private final ClosableResources<?> closableResources;
        private final MoveToState moveToState = new MoveToState();
        long index; // index of the next read.
        @Nullable
        WireStore store;
        private int cycle;
        private TailerDirection direction = TailerDirection.FORWARD;
        private Wire wireForIndex;
        private boolean readAfterReplicaAcknowledged;
        @NotNull
        private TailerState state = UNINITIALISED;
        private long indexAtCreation = Long.MIN_VALUE;
        private boolean readingDocumentFound = false;
        private long address = NoBytesStore.NO_PAGE;

        public StoreTailer(@NotNull final SingleChronicleQueue queue) {
            this(queue, null);
        }

        public StoreTailer(@NotNull final SingleChronicleQueue queue, LongValue indexValue) {
            this.queue = queue;
            this.indexValue = indexValue;
            this.setCycle(Integer.MIN_VALUE);
            this.index = 0;
            queue.addCloseListener(this, StoreTailer::close);
            closableResources = new ClosableResources<>(queue);

            if (indexValue == null) {
                toStart();
            } else {
                moveToIndex(indexValue.getVolatileValue());
            }
        }

        @Nullable
        public static MessageHistory readHistory(final DocumentContext dc, MessageHistory history) {
            final Wire wire = dc.wire();

            if (wire == null)
                return null;

            Object parent = wire.parent();
            wire.parent(null);
            try {
                final Bytes<?> bytes = wire.bytes();

                final byte code = bytes.readByte(bytes.readPosition());
                history.reset();

                return code == (byte) FIELD_NUMBER ?
                        readHistoryFromBytes(wire, history) :
                        readHistoryFromWire(wire, history);
            } finally {
                wire.parent(parent);
            }

        }

        private static MessageHistory readHistoryFromBytes(final Wire wire, MessageHistory history) {
            final Bytes<?> bytes = wire.bytes();
            if (MESSAGE_HISTORY_METHOD_ID != wire.readEventNumber())
                return null;
            ((BytesMarshallable) history).readMarshallable(bytes);
            return history;
        }

        private static MessageHistory readHistoryFromWire(final Wire wire, MessageHistory history) {
            final StringBuilder sb = SBP.acquireStringBuilder();
            ValueIn valueIn = wire.read(sb);

            if (!MethodReader.HISTORY.contentEquals(sb))
                return null;
            valueIn.object(history, MessageHistory.class);
            return history;
        }

        @Override
        public boolean readDocument(@NotNull ReadMarshallable reader) {
            try (@NotNull DocumentContext dc = readingDocument(false)) {
                if (!dc.isPresent())
                    return false;
                reader.readMarshallable(dc.wire());
            }
            return true;
        }

        @Override
        @NotNull
        public DocumentContext readingDocument() {
            // trying to create an initial document without a direction should not consume a message
            long index = index();
            if (direction == NONE && (index == indexAtCreation || index == 0) && !readingDocumentFound) {
                return net.openhft.chronicle.wire.NoDocumentContext.INSTANCE;
            }
            return readingDocument(false);
        }

        private void close() {
            // the wire ref count will be released here by setting it to null
            context.wire(null);
            Wire w0 = wireForIndex;
            if (w0 != null)
                w0.bytes().release();
            wireForIndex = null;
            if (store != null) {
                queue.release(store);
            }
            store = null;
        }

        @Override
        public Wire wire() {
            return context.wire();
        }

        @Override
        public Wire wireForIndex() {
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

        @NotNull
        @Override
        public String toString() {
            long index = index();
            return "StoreTailer{" +
                    "index sequence=" + queue.rollCycle().toSequenceNumber(index) +
                    ", index cycle=" + queue.rollCycle().toCycle(index) +
                    ", store=" + store + ", queue=" + queue + '}';
        }

        @NotNull
        @Override
        public DocumentContext readingDocument(boolean includeMetaData) {
            Jvm.optionalSafepoint();

            if (queue.isClosed.get())
                throw new IllegalStateException("Queue is closed");
            try {
                Jvm.optionalSafepoint();
                boolean next = false, tryAgain = true;
                if (state == FOUND_CYCLE) {
                    try {
                        Jvm.optionalSafepoint();
                        next = inACycle(includeMetaData);
                        Jvm.optionalSafepoint();

                        tryAgain = false;
                    } catch (EOFException eof) {
                        state = TailerState.END_OF_CYCLE;
                    }
                }
                Jvm.optionalSafepoint();

                if (tryAgain)
                    next = next0(includeMetaData);

                Jvm.optionalSafepoint();
                if (context.present(next)) {
                    Bytes<?> bytes = context.wire().bytes();
                    context.setStart(bytes.readPosition() - 4);
                    readingDocumentFound = true;
                    address = bytes.addressForRead(bytes.readPosition(), 4);
                    Jvm.optionalSafepoint();
                    return context;
                }
                Jvm.optionalSafepoint();

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
                if (context.wire() == null) {
                    address = NoBytesStore.NO_PAGE;
                } else {
                    Bytes<?> bytes = context.wire().bytes();
                    address = bytes.addressForRead(bytes.readPosition(), 4);
                }
            } catch (StreamCorruptedException e) {
                throw new IllegalStateException(e);
            } catch (UnrecoverableTimeoutException notComplete) {
                // so treat as empty.
            } catch (DecoratedBufferUnderflowException e) {
                // read-only tailer view is fixed, a writer could continue past the end of the view
                // at the point this tailer was created. Log a warning and return no document.
                if (queue.isReadOnly()) {
                    Jvm.warn().on(StoreTailer.class, "Tried to read past the end of a read-only view. " +
                            "Underlying data store may have grown since this tailer was created.", e);
                } else {
                    throw e;
                }
            }
            return net.openhft.chronicle.wire.NoDocumentContext.INSTANCE;
        }

        @SuppressWarnings("restriction")
        @Override
        public boolean peekDocument() {
            int header = UnsafeMemory.UNSAFE.getIntVolatile(null, address);
            return header > 0x0 | header == Wires.END_OF_DATA;
        }

        private boolean next0(boolean includeMetaData) throws UnrecoverableTimeoutException, StreamCorruptedException {
            for (int i = 0; i < 1000; i++) {
                switch (state) {
                    case UNINITIALISED:
                        final long firstIndex = queue.firstIndex();
                        if (firstIndex == Long.MAX_VALUE)
                            return false;
                        if (!moveToIndexInternal(firstIndex))
                            return false;
                        break;

                    case FOUND_CYCLE: {
                        try {
                            return inACycle(includeMetaData);
                        } catch (EOFException eof) {
                            state = TailerState.END_OF_CYCLE;
                        }
                        break;
                    }

                    case END_OF_CYCLE:
                        if (endOfCycle())
                            continue;
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

        private boolean endOfCycle() {
            long oldIndex = this.index();
            int currentCycle = queue.rollCycle().toCycle(oldIndex);
            long nextIndex = nextIndexWithNextAvailableCycle(currentCycle);

            if (nextIndex != Long.MIN_VALUE) {
                return nextEndOfCycle(nextIndex);
            } else {
                state = END_OF_CYCLE;
            }
            return false;
        }

        private boolean beyondStartOfCycle() throws StreamCorruptedException {
            if (direction == FORWARD) {
                state = UNINITIALISED;
                return true;
            } else if (direction == BACKWARD) {
                return beyondStartOfCycleBackward();
            }
            throw new AssertionError("direction not set, direction=" + direction);
        }

        private boolean nextEndOfCycle(long nextIndex) {
            if (moveToIndexInternal(nextIndex)) {
                state = FOUND_CYCLE;
                return true;
            }
            if (state == END_OF_CYCLE)
                return true;
            if (cycle < queue.lastCycle()) {
                // we have encountered an empty file without an EOF marker
                // TODO: more work needed - I thought that the appender and/or tailer would write an EOF into this file
                state = END_OF_CYCLE;
                return true;
            }
            // We are here because we are waiting for an entry to be written to this file.
            // Winding back to the previous cycle results in a re-initialisation of all the objects => garbage
            int nextCycle = queue.rollCycle().toCycle(nextIndex);
            cycle(nextCycle);
            state = CYCLE_NOT_FOUND;
            return false;
        }

        private boolean beyondStartOfCycleBackward() throws StreamCorruptedException {
            // give the position of the last entry and
            // flag we want to count it even though we don't know if it will be meta data or not.

            boolean foundCycle = cycle(queue.rollCycle().toCycle(index()));

            if (foundCycle) {
                long lastSequenceNumberInThisCycle = store().sequenceForPosition(this, Long.MAX_VALUE, false);
                long nextIndex = queue.rollCycle().toIndex(this.cycle,
                        lastSequenceNumberInThisCycle);
                moveToIndexInternal(nextIndex);
                state = FOUND_CYCLE;
                return true;
            }

            int cycle = queue.rollCycle().toCycle(index());
            long nextIndex = nextIndexWithNextAvailableCycle(cycle);

            if (nextIndex != Long.MIN_VALUE) {
                moveToIndexInternal(nextIndex);
                state = FOUND_CYCLE;
                return true;
            }

            state = BEYOND_START_OF_CYCLE;
            return false;
        }

        private boolean nextCycleNotFound() {
            if (index() == Long.MIN_VALUE) {
                if (this.store != null)
                    queue.release(this.store);
                this.store = null;
                closableResources.storeReference = null;
                return false;
            }

            if (moveToIndexInternal(index())) {
                state = FOUND_CYCLE;
                return true;
            }
            return false;
        }

        private boolean inACycle(boolean includeMetaData)
                throws EOFException {
            Jvm.optionalSafepoint();
            if (readAfterReplicaAcknowledged && inACycleCheckRep()) return false;

            Jvm.optionalSafepoint();
            if (direction != TailerDirection.FORWARD && !inACycleNotForward()) return false;
            Jvm.optionalSafepoint();

            Wire wire = wire();
            Bytes<?> bytes = wire.bytes();
            bytes.readLimit(bytes.capacity());

            switch (wire.readDataHeader(includeMetaData)) {
                case NONE:
                    Jvm.optionalSafepoint();
                    // no more polling - appender will always write (or recover) EOF
                    return false;
                case META_DATA:
                    Jvm.optionalSafepoint();
                    context.metaData(true);
                    break;
                case DATA:
                    Jvm.optionalSafepoint();
                    context.metaData(false);
                    break;
            }

            Jvm.optionalSafepoint();
            inACycleFound(bytes);
            Jvm.optionalSafepoint();
            return true;
        }

        private boolean inACycleCheckRep() {
            long lastSequenceAck = queue.lastAcknowledgedIndexReplicated();
            long seq = queue.rollCycle().toSequenceNumber(index());
            return seq > lastSequenceAck;
        }

        private boolean inACycleNotForward() {
            Jvm.optionalSafepoint();
            if (!moveToIndexInternal(index())) {
                try {
                    Jvm.optionalSafepoint();
                    // after toEnd() call, index is past the end of the queue
                    // so try to go back one (to the last record in the queue)
                    if ((int) queue.rollCycle().toSequenceNumber(index()) < 0) {
                        long lastSeqNum = store.lastSequenceNumber(this);
                        if (lastSeqNum == -1) {
                            windBackCycle(cycle);
                            return moveToIndexInternal(index());
                        }

                        return moveToIndexInternal(queue.rollCycle().toIndex(cycle, lastSeqNum));
                    }
                    if (!moveToIndexInternal(index() - 1)) {
                        Jvm.optionalSafepoint();
                        return false;
                    }
                } catch (Exception e) {
                    // can happen if index goes negative
                    Jvm.optionalSafepoint();
                    return false;
                }
            }
            Jvm.optionalSafepoint();
            return true;
        }

        private void inACycleFound(Bytes<?> bytes) {
            context.closeReadLimit(bytes.capacity());
            wire().readAndSetLength(bytes.readPosition());
            long end = bytes.readLimit();
            context.closeReadPosition(end);
            Jvm.optionalSafepoint();
        }

        private long nextIndexWithNextAvailableCycle(int cycle) {
            assert cycle != Integer.MIN_VALUE : "cycle == Integer.MIN_VALUE";

            if (cycle > queue.lastCycle() || direction == TailerDirection.NONE) {
                return Long.MIN_VALUE;
            }

            long nextIndex;
            int nextCycle = cycle + direction.add();
            boolean found = cycle(nextCycle);
            if (found)
                nextIndex = nextIndexWithinFoundCycle(nextCycle);
            else
                try {
                    int nextCycle0 = queue.nextCycle(this.cycle, direction);
                    if (nextCycle0 == -1)
                        return Long.MIN_VALUE;

                    nextIndex = nextIndexWithinFoundCycle(nextCycle0);

                } catch (ParseException e) {
                    throw new IllegalStateException(e);
                }

            if (LOG.isDebugEnabled()) {
                int nextIndexCycle = queue.rollCycle().toCycle(nextIndex);
                if (nextIndex != Long.MIN_VALUE && nextIndexCycle - 1 != cycle) {

                    /*
                     * lets say that you were using a roll cycle of TEST_SECONDLY
                     * and you wrote a message to the queue, if you created a tailer and read the first message,
                     * then waited around 22 seconds before writing the next message, when the tailer
                     * came to read the next message, there would be a gap of 22 cycle files
                     * that did not exist, that is what this is reporting. If you are using daily rolling,
                     * and writing every day, you should not see this message.
                     */

                    LOG.debug("Rolled " + (nextIndexCycle - cycle) + " " + "times to find the " +
                            "next cycle file. This can occur if your appenders have not written " +
                            "anything for a while, leaving the cycle files with a gap.");
                }
            }

            return nextIndex;
        }

        private long nextIndexWithinFoundCycle(int nextCycle) {
            state = FOUND_CYCLE;
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
         * @return provides an index that includes the cycle number
         */
        @Override
        public long index() {
            return indexValue == null ? this.index : indexValue.getValue();
        }

        @Override
        public int cycle() {
            return this.cycle;
        }

        @Override
        public boolean moveToIndex(final long index) {
            if (moveToState.canReuseLastIndexMove(index, state, direction, queue, wire())) {
                return true;
            } else if (moveToState.indexIsCloseToAndAheadOfLastIndexMove(index, state, direction, queue)) {
                final long knownIndex = moveToState.lastMovedToIndex;
                final boolean found =
                        this.store.linearScanTo(index, knownIndex, this,
                                moveToState.readPositionAtLastMove) == ScanResult.FOUND;
                if (found) {
                    index(index);
                    moveToState.onSuccessfulScan(index, direction, wire().bytes().readPosition());
                }
                return found;
            }

            return moveToIndexInternal(index);
        }

        ScanResult moveToIndexResult(long index) {
            final int cycle = queue.rollCycle().toCycle(index);
            final long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
            if (LOG.isTraceEnabled()) {
                Jvm.debug().on(getClass(), "moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
            }

            if (cycle != this.cycle || state != FOUND_CYCLE) {
                // moves to the expected cycle
                if (!cycle(cycle))
                    return ScanResult.NOT_REACHED;
            }

            index(index);
            ScanResult scanResult = this.store().moveToIndexForRead(this, sequenceNumber);
            Bytes<?> bytes = wire().bytes();
            if (scanResult == FOUND) {
                state = FOUND_CYCLE;
                moveToState.onSuccessfulLookup(index, direction, bytes.readPosition());
                return scanResult;
            } else if (scanResult == END_OF_FILE) {
                state = END_OF_CYCLE;
                return scanResult;
            } else if (scanResult == NOT_FOUND && this.cycle < this.queue.lastCycle) {
                state = END_OF_CYCLE;
                return END_OF_FILE;
            }

            return scanResult;
        }

        @NotNull
        @Override
        public final ExcerptTailer toStart() {
            assert direction != BACKWARD;
            final int firstCycle = queue.firstCycle();
            if (firstCycle == Integer.MAX_VALUE) {
                state = UNINITIALISED;
                return this;
            }
            if (firstCycle != this.cycle) {
                // moves to the expected cycle
                boolean found = cycle(firstCycle);
                assert found || store == null;
                if (found)
                    state = FOUND_CYCLE;
            }
            index(queue.rollCycle().toIndex(cycle, 0));

            state = FOUND_CYCLE;
            if (wire() != null) {
                wire().bytes().readPosition(0);
                address = wire().bytes().addressForRead(0);
            }
            return this;
        }

        private boolean moveToIndexInternal(final long index) {
            moveToState.indexMoveCount++;
            Jvm.optionalSafepoint();
            final ScanResult scanResult = moveToIndexResult(index);
            Jvm.optionalSafepoint();
            return scanResult == FOUND;
        }

        /**
         * gives approximately the last index, can not be relied on as the last index may have changed just after this was called. For this reason,
         * this code is not in queue as it should only be an internal method
         *
         * @return the last index at the time this method was called, or Long.MIN_VALUE if none.
         */
        private long approximateLastIndex() {

            RollCycle rollCycle = queue.rollCycle();
            final int lastCycle = queue.lastCycle();
            try {
                if (lastCycle == Integer.MIN_VALUE)
                    return Long.MIN_VALUE;

                final WireStore wireStore = queue.storeForCycle(lastCycle, queue.epoch(), false);
                this.setCycle(lastCycle);
                if (wireStore == null)
                    throw new IllegalStateException("Store not found for cycle " + Long.toHexString(lastCycle) + ". Probably the files were removed?");

                if (store != null)
                    queue.release(store);

                if (this.store != wireStore) {
                    this.store = wireStore;
                    closableResources.storeReference = wireStore;
                    resetWires();
                }
                // give the position of the last entry and
                // flag we want to count it even though we don't know if it will be meta data or not.

                long sequenceNumber = store.lastSequenceNumber(this);

                // fixes #378
                if (sequenceNumber == -1L) {
                    // nothing has been written yet, so point to start of cycle
                    return rollCycle.toIndex(lastCycle, 0L);
                }
                return rollCycle.toIndex(lastCycle, sequenceNumber);

            } catch (@NotNull StreamCorruptedException | UnrecoverableTimeoutException e) {
                throw new IllegalStateException(e);
            }
        }

        private boolean headerNumberCheck(@NotNull AbstractWire wire) {

            wire.headNumberCheck((actual, position) -> {
                try {
                    long expecting = store.sequenceForPosition(this, position, false);
                    if (actual == expecting)
                        return true;
                    LOG.error("", new AssertionError("header number check failed " +
                            "expecting=" + expecting +
                            "  !=  actual=" + actual));

                    return false;
                } catch (Exception e) {
                    LOG.error("", e);
                    return false;
                }
            });

            return true;
        }

        private void resetWires() {
            WireType wireType = queue.wireType();

            final AbstractWire wire = (AbstractWire) readAnywhere(wireType.apply(store.bytes()));
            assert headerNumberCheck(wire);
            this.context.wire(wire);
            wire.parent(this);

            Wire wireForIndexOld = wireForIndex;
            wireForIndex = readAnywhere(wireType.apply(store().bytes()));
            closableResources.wireForIndexReference = wireForIndex.bytes();
            closableResources.wireReference = wire.bytes();
            assert headerNumberCheck((AbstractWire) wireForIndex);
            assert wire != wireForIndexOld;

            if (wireForIndexOld != null) {
                releaseWireResources(wireForIndexOld);
            }

        }

        @NotNull
        private Wire readAnywhere(@NotNull Wire wire) {
            Bytes<?> bytes = wire.bytes();
            bytes.readLimit(bytes.capacity());
            return wire;
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() {
            if (direction.equals(TailerDirection.BACKWARD))
                return originalToEnd();

            return optimizedToEnd();
        }

        @NotNull
        private ExcerptTailer optimizedToEnd() {
            RollCycle rollCycle = queue.rollCycle();
            final int lastCycle = queue.lastCycle();
            try {
                if (lastCycle == Integer.MIN_VALUE) {
                    if (state() == TailerState.CYCLE_NOT_FOUND)
                        state = UNINITIALISED;
                    return this;
                }

                final WireStore wireStore = queue.storeForCycle(lastCycle, queue.epoch(), false);
                this.setCycle(lastCycle);
                if (wireStore == null)
                    throw new IllegalStateException("Store not found for cycle " + Long.toHexString(lastCycle) + ". Probably the files were removed?");

                if (store != null)
                    queue.release(store);

                if (this.store != wireStore) {
                    this.store = wireStore;
                    closableResources.storeReference = wireStore;
                    resetWires();
                }
                // give the position of the last entry and
                // flag we want to count it even though we don't know if it will be meta data or not.

                long sequenceNumber = store.moveToEndForRead(wire());

                // fixes #378
                if (sequenceNumber == -1L) {
                    // nothing has been written yet, so point to start of cycle
                    return originalToEnd();
                }

                if (Wires.isEndOfFile(wire().bytes().readVolatileInt(wire().bytes().readPosition()))) {
                    state = END_OF_CYCLE;
                } else
                    state = FOUND_CYCLE;

                index(rollCycle.toIndex(lastCycle, sequenceNumber));

            } catch (@NotNull UnrecoverableTimeoutException e) {
                throw new IllegalStateException(e);
            }

            return this;
        }

        @NotNull

        public ExcerptTailer originalToEnd() {
            long index = approximateLastIndex();

            if (index == Long.MIN_VALUE) {
                if (state() == TailerState.CYCLE_NOT_FOUND)
                    state = UNINITIALISED;
                return this;
            }
            final ScanResult scanResult = moveToIndexResult(index);
            switch (scanResult) {
                case NOT_FOUND:
                    if (moveToIndexResult(index - 1) == FOUND)
                        state = FOUND_CYCLE;
                    break;

                case FOUND:
                    if (direction == FORWARD) {
                        final ScanResult result = moveToIndexResult(++index);
                        switch (result) {
                            case FOUND:
                                // the end moved!!
                                state = FOUND_CYCLE;
                                break;
                            case NOT_REACHED:
                                throw new IllegalStateException("NOT_REACHED after FOUND");
                            case NOT_FOUND:
                                state = FOUND_CYCLE;
                                break;
                            case END_OF_FILE:
                                state = END_OF_CYCLE;
                                break;
                            default:
                                throw new IllegalStateException("Unknown ScanResult: " + result);
                        }
                    }
                    break;
                case NOT_REACHED:
                    approximateLastIndex();
                    throw new IllegalStateException("NOT_REACHED index: " + Long.toHexString(index));
                case END_OF_FILE:
                    state = END_OF_CYCLE;
                    break;
                default:
                    throw new IllegalStateException("Unknown ScanResult: " + scanResult);

            }

            return this;

        }

        @Override
        public TailerDirection direction() {
            return direction;
        }

        @NotNull
        @Override
        public ExcerptTailer direction(TailerDirection direction) {
            final TailerDirection oldDirection = this.direction();
            this.direction = direction;
            if (oldDirection == TailerDirection.BACKWARD &&
                    direction == TailerDirection.FORWARD) {
                moveToIndexInternal(index());
            }

            return this;
        }

        @Override
        @NotNull
        public ChronicleQueue queue() {
            return queue;
        }

        @Override
        public Runnable getCloserJob() {
            return closableResources::releaseResources;
        }

        /**
         * Can be used to manually release resources when this StoreTailer is no longer used.
         */
        public void releaseResources() {
            queue.removeCloseListener(this);
            getCloserJob().run();
        }

        @PackageLocal
        void incrementIndex() {
            RollCycle rollCycle = queue.rollCycle();
            long index = this.index();
            long seq = rollCycle.toSequenceNumber(index);
            int cycle = rollCycle.toCycle(index);

            seq += direction.add();
            switch (direction) {
                case NONE:
                    break;
                case FORWARD:
                    // if it runs out of seq number it will flow over to tomorrows cycle file
                    if (rollCycle.toSequenceNumber(seq) < seq) {
                        cycle(cycle + 1);
                        LOG.warn("we have run out of sequence numbers, so will start to write to " +
                                "the next .cq4 file, the new cycle=" + cycle);
                        seq = 0;
                    }
                    break;
                case BACKWARD:
                    if (seq < 0) {
                        windBackCycle(cycle);
                        return;
                    }
                    break;
            }
            index0(rollCycle.toIndex(cycle, seq));

        }

        private void windBackCycle(int cycle) {
            if (tryWindBack(cycle - 1))
                return;
            cycle--;
            for (long first = queue.firstCycle(); cycle >= first; cycle--) {
                if (tryWindBack(cycle))
                    return;
            }
            this.index(queue.rollCycle().toIndex(cycle, -1));
            this.state = BEYOND_START_OF_CYCLE;
        }

        private boolean tryWindBack(int cycle) {
            long count = queue.exceptsPerCycle(cycle);
            if (count <= 0)
                return false;
            RollCycle rollCycle = queue.rollCycle();
            moveToIndexInternal(rollCycle.toIndex(cycle, count - 1));
            this.state = FOUND_CYCLE;
            return true;
        }

        void index0(long index) {
            if (indexValue == null)
                this.index = index;
            else
                indexValue.setValue(index);
        }

        // DON'T INLINE THIS METHOD, as it's used by enterprise chronicle queue
        void index(long index) {
            index0(index);

            if (indexAtCreation == Long.MIN_VALUE) {
                indexAtCreation = index;
            }

            moveToState.reset();
        }

        private boolean cycle(final int cycle) {
            if (this.cycle == cycle && state == FOUND_CYCLE)
                return true;

            WireStore nextStore = this.queue.storeForCycle(cycle, queue.epoch(), false);

            if (nextStore == null && this.store == null)
                return false;

            if (nextStore == null) {
                if (direction == BACKWARD)
                    state = BEYOND_START_OF_CYCLE;
                else
                    state = CYCLE_NOT_FOUND;
                return false;
            }

            if (store != null)
                queue.release(store);

            if (nextStore == this.store)
                return true;

            context.wire(null);
            this.store = nextStore;
            closableResources.storeReference = nextStore;
            this.state = FOUND_CYCLE;
            this.setCycle(cycle);
            resetWires();
            final Wire wire = wire();
            wire.parent(this);
            wire.pauser(queue.pauserSupplier.get());
            return true;
        }

        void release() {
            if (store != null) {
                queue.release(store);
                store = null;
                closableResources.storeReference = null;
            }
            state = UNINITIALISED;
        }

        @Override
        public void readAfterReplicaAcknowledged(boolean readAfterReplicaAcknowledged) {
            this.readAfterReplicaAcknowledged = readAfterReplicaAcknowledged;
        }

        @Override
        public boolean readAfterReplicaAcknowledged() {
            return readAfterReplicaAcknowledged;
        }

        @NotNull
        @Override
        public TailerState state() {
            return state;
        }

        @NotNull
        @Override
        public ExcerptTailer afterLastWritten(@NotNull ChronicleQueue queue) {
            if (queue == this.queue)
                throw new IllegalArgumentException("You must pass the queue written to, not the queue read");
            @NotNull ExcerptTailer tailer = queue.createTailer()
                    .direction(BACKWARD)
                    .toEnd();

            @NotNull VanillaMessageHistory messageHistory = new VanillaMessageHistory();

            while (true) {
                try (DocumentContext context = tailer.readingDocument()) {
                    if (!context.isPresent()) {
                        toStart();
                        return this;
                    }

                    MessageHistory veh = readHistory(context, messageHistory);
                    if (veh == null)
                        continue;

                    int i = veh.sources() - 1;
                    if (i < 0)
                        continue;
                    if (veh.sourceId(i) != this.sourceId())
                        continue;

                    long sourceIndex = veh.sourceIndex(i);
                    if (!moveToIndexInternal(sourceIndex)) {
                        final String errorMessage = String.format(
                                "Unable to move to sourceIndex %s in queue %s",
                                Long.toHexString(sourceIndex), this.queue.fileAbsolutePath());
                        throw new IORuntimeException(errorMessage + extraInfo(tailer, messageHistory));
                    }
                    try (DocumentContext content = readingDocument()) {
                        if (!content.isPresent()) {
                            final String errorMessage = String.format(
                                    "No readable document found at sourceIndex %s in queue %s",
                                    Long.toHexString(sourceIndex + 1), this.queue.fileAbsolutePath());
                            throw new IORuntimeException(errorMessage + extraInfo(tailer, messageHistory));
                        }
                        // skip this message and go to the next.
                    }
                    return this;
                }
            }
        }

        private String extraInfo(@NotNull ExcerptTailer tailer, @NotNull VanillaMessageHistory messageHistory) {
            return String.format(
                    ". That sourceIndex was determined fom the last entry written to queue %s " +
                            "(message index %s, message history %s). If source queue is replicated then " +
                            "sourceIndex may not have been replicated yet",
                    tailer.queue().fileAbsolutePath(), Long.toHexString(tailer.index()), WireType.TEXT.asString(messageHistory));
        }

        public void setCycle(int cycle) {
            this.cycle = cycle;

        }

        // visible for testing
        int getIndexMoveCount() {
            return moveToState.indexMoveCount;
        }

        @Deprecated // Should not be providing accessors to reference-counted objects
        @NotNull
        WireStore store() {
            if (store == null)
                setCycle(cycle());
            return store;
        }

        private static final class MoveToState {
            private long lastMovedToIndex = Long.MIN_VALUE;
            private TailerDirection directionAtLastMoveTo = TailerDirection.NONE;
            private long readPositionAtLastMove = Long.MIN_VALUE;
            private int indexMoveCount = 0;

            void onSuccessfulLookup(
                    final long movedToIndex, final TailerDirection direction,
                    final long readPosition) {
                this.lastMovedToIndex = movedToIndex;
                this.directionAtLastMoveTo = direction;
                this.readPositionAtLastMove = readPosition;
            }

            void onSuccessfulScan(
                    final long movedToIndex, final TailerDirection direction,
                    final long readPosition) {
                this.lastMovedToIndex = movedToIndex;
                this.directionAtLastMoveTo = direction;
                this.readPositionAtLastMove = readPosition;
            }

            void reset() {
                lastMovedToIndex = Long.MIN_VALUE;
                directionAtLastMoveTo = TailerDirection.NONE;
                readPositionAtLastMove = Long.MIN_VALUE;
            }

            private boolean indexIsCloseToAndAheadOfLastIndexMove(
                    final long index, final TailerState state, final TailerDirection direction,
                    final ChronicleQueue queue) {
                return lastMovedToIndex != Long.MIN_VALUE &&
                        index - lastMovedToIndex < INDEXING_LINEAR_SCAN_THRESHOLD &&
                        state == FOUND_CYCLE &&
                        direction == directionAtLastMoveTo &&
                        queue.rollCycle().toCycle(index) == queue.rollCycle().toCycle(lastMovedToIndex) &&
                        index > lastMovedToIndex;
            }

            private boolean canReuseLastIndexMove(
                    final long index, final TailerState state, final TailerDirection direction,
                    final ChronicleQueue queue, final Wire wire) {

                return ((wire == null) || wire.bytes().readPosition() == readPositionAtLastMove) &&
                        index == this.lastMovedToIndex && index != 0 && state == FOUND_CYCLE &&
                        direction == directionAtLastMoveTo &&
                        queue.rollCycle().toCycle(index) == queue.rollCycle().toCycle(lastMovedToIndex);
            }

        }

        class StoreTailerContext extends BinaryReadDocumentContext {

            boolean rollbackOnClose = false;

            StoreTailerContext() {
                super(null);
            }

            @Override
            public void rollbackOnClose() {
                rollbackOnClose = true;
            }

            @Override
            public long index() {
                return StoreTailer.this.index();
            }

            @Override
            public int sourceId() {
                return StoreTailer.this.sourceId();
            }

            @Override
            public void close() {

                try {
                    if (rollbackOnClose) {
                        present = false;
                        if (start != -1)
                            wire.bytes().readPosition(start).readLimit(readLimit);
                        start = -1;
                        return;
                    }

                    if (isPresent() && !isMetaData())
                        incrementIndex();

                    super.close();
                    // assert wire == null || wire.endUse();

                } finally {
                    rollbackOnClose = false;
                }
            }

            boolean present(boolean present) {
                return this.present = present;
            }

            public void wire(@Nullable AbstractWire wire) {
                if (wire == this.wire)
                    return;

                AbstractWire oldWire = this.wire;
                this.wire = wire;

                if (oldWire != null) {
                    releaseWireResources(oldWire);
                }
            }
        }
    }
}