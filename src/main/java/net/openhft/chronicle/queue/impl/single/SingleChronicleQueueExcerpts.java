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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.bytes.util.DecoratedBufferUnderflowException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.TailerState;
import net.openhft.chronicle.queue.impl.CommonStore;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.queue.impl.WireStorePool;
import net.openhft.chronicle.wire.AbstractWire;
import net.openhft.chronicle.wire.BinaryReadDocumentContext;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.SourceContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.VanillaMessageHistory;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.BufferOverflowException;
import java.text.ParseException;

import static net.openhft.chronicle.queue.TailerDirection.BACKWARD;
import static net.openhft.chronicle.queue.TailerDirection.FORWARD;
import static net.openhft.chronicle.queue.TailerDirection.NONE;
import static net.openhft.chronicle.queue.TailerState.BEYOND_START_OF_CYCLE;
import static net.openhft.chronicle.queue.TailerState.CYCLE_NOT_FOUND;
import static net.openhft.chronicle.queue.TailerState.END_OF_CYCLE;
import static net.openhft.chronicle.queue.TailerState.FOUND_CYCLE;
import static net.openhft.chronicle.queue.TailerState.UNINITIALISED;
import static net.openhft.chronicle.queue.impl.single.ScanResult.END_OF_FILE;
import static net.openhft.chronicle.queue.impl.single.ScanResult.FOUND;

public class SingleChronicleQueueExcerpts {
    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueueExcerpts.class);

    @FunctionalInterface
    interface WireWriter<T> {
        void write(T message, WireOut wireOut);
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
        static final int REPEAT_WHILE_ROLLING = 128;
        @NotNull
        private final SingleChronicleQueue queue;
        @NotNull
        private final StoreAppenderContext context;
        private final ClosableResources closableResources;
        @NotNull
        private final HeaderWriteStrategy headerWriteStrategy;
        private final WireStorePool storePool;
        @Nullable
        WireStore store;
        private int cycle = Integer.MIN_VALUE;
        @Nullable
        private Wire wire;
        @Nullable
        private Wire bufferWire; // if you have a buffered write.
        @Nullable
        private Wire wireForIndex;
        private long position = 0;
        @Nullable
        private volatile Thread appendingThread = null;
        private long lastIndex = Long.MIN_VALUE;
        private boolean lazyIndexing = false;
        private long lastPosition;
        private int lastCycle;
        @Nullable
        private PretoucherState pretoucher = null;
        private Padding padToCacheLines = Padding.SMART;

        StoreAppender(@NotNull SingleChronicleQueue queue, boolean progressOnContention, @NotNull WireStorePool storePool) {
            this.queue = queue;
            queue.addCloseListener(this, StoreAppender::close);
            context = new StoreAppenderContext();

            closableResources = new ClosableResources(queue);
            queue.ensureThatRollCycleDoesNotConflictWithExistingQueueFiles();
            headerWriteStrategy = progressOnContention ? new HeaderWriteStrategyDefer() : new HeaderWriteStrategyOriginal();
            this.storePool = storePool;
        }

        @Override
        @NotNull
        public Padding padToCacheAlignMode() {
            return padToCacheLines;
        }

        /**
         * @param padToCacheLines the default for chronicle queue is Padding.SMART, which
         *                        automatically pads all method calls other than {@link
         *                        StoreAppender#writeBytes(net.openhft.chronicle.bytes.WriteBytesMarshallable)}
         *                        and   {@link StoreAppender#writeText(java.lang.CharSequence)}.
         *                        Which can not be padded with out changing the message format, The
         *                        reason we pad is to ensure that a message header does not straggle
         *                        a cache line.
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
                marshallable.writeMarshallable(dc.wire().bytes());
                if (padToCacheAlignMode() != Padding.ALWAYS)
                    ((StoreAppenderContext) dc).padToCacheAlign = false;
            }
        }

        @Override
        public void writeText(@NotNull CharSequence text) throws UnrecoverableTimeoutException {
            try (DocumentContext dc = writingDocument()) {
                dc.wire().bytes()
                        .append8bit(text);
                if (padToCacheAlignMode() != Padding.ALWAYS)
                    ((StoreAppenderContext) dc).padToCacheAlign = false;
            }
        }

        void close() {
            Wire w0 = wireForIndex;
            wireForIndex = null;
            if (w0 != null)
                w0.bytes().release();
            Wire w = wire;
            wire = null;
            if (w != null)
                w.bytes().release();
            if (store != null) {
                storePool.release(store);
            }
            if (bufferWire != null) {
                bufferWire.bytes().release();
                bufferWire = null;
            }
            store = null;
            storePool.close();
        }

        @Override
        public void pretouch() {
            setCycle(queue.cycle());
            if (pretoucher == null)
                pretoucher = new PretoucherState(() -> this.store.writePosition());
            Wire wire = this.wire;
            if (wire != null)
                pretoucher.pretouch((MappedBytes) wire.bytes());
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

        @NotNull
        @Override
        public ExcerptAppender lazyIndexing(boolean lazyIndexing) {
            this.lazyIndexing = lazyIndexing;
            try {
                resetPosition();
            } catch (EOFException ex) {
                throw new IllegalStateException("EOF found");
            }
            return this;
        }

        @Override
        public boolean lazyIndexing() {
            return lazyIndexing;
        }

        @Override
        public boolean recordHistory() {
            return sourceId() != 0;
        }

        private void setCycle(int cycle) {
            if (cycle != this.cycle)
                setCycle2(cycle, true);
        }

        private void setCycle2(int cycle, boolean createIfAbsent) {
            if (cycle < 0)
                throw new IllegalArgumentException("You can not have a cycle that starts " +
                        "before Epoch. cycle=" + cycle);

            SingleChronicleQueue queue = this.queue;

            if (this.store != null) {
                storePool.release(this.store);
            }

            this.store = storePool.acquire(cycle, queue.epoch(), createIfAbsent);
            closableResources.storeReference = store;
            resetWires(queue);

            // only set the cycle after the wire is set.
            this.cycle = cycle;
            assert wire.startUse();
            wire.parent(this);
            wire.pauser(queue.pauserSupplier.get());
            try {
                resetPosition();
                queue.onRoll(cycle);
            } catch (EOFException eof) {
                handleRoll(cycle);
            }
        }

        private void resetWires(@NotNull SingleChronicleQueue queue) {
            WireType wireType = queue.wireType();
            {
                Wire oldw = this.wire;
                this.wire = wireType.apply(store.bytes());
                closableResources.wireReference = this.wire.bytes();

                if (oldw != null) {
                    releaseWireResources(oldw);
                }
            }
            {
                Wire old = this.wireForIndex;
                this.wireForIndex = wireType.apply(store.bytes());
                closableResources.wireForIndexReference = wireForIndex.bytes();

                if (old != null) {
                    releaseWireResources(old);
                }
            }

        }

        private void resetPosition() throws UnrecoverableTimeoutException, EOFException {
            try {

                if (store == null || wire == null)
                    return;
                position(store.writePosition());

                Bytes<?> bytes = wire.bytes();
                int header = bytes.readVolatileInt(position);
                if (header == Wires.END_OF_DATA) {
                    throw new EOFException();
                }
                assert position == 0 || Wires.isReadyData(header);
                bytes.writePosition(position + 4 + Wires.lengthOf(header));
                if (lazyIndexing) {
                    wire.headerNumber(Long.MIN_VALUE);
                    return;
                }

                final long headerNumber = store.sequenceForPosition(this, position, true);
                wire.headerNumber(queue.rollCycle().toIndex(cycle, headerNumber + 1) - 1);
                assert lazyIndexing || wire.headerNumber() != -1 || checkIndex(wire.headerNumber(), position);

            } catch (@NotNull BufferOverflowException | StreamCorruptedException e) {
                throw new AssertionError(e);
            }
            assert checkWritePositionHeaderNumber();
        }

        @NotNull
        @Override
        public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
            assert checkAppendingThread();
            assert checkWritePositionHeaderNumber();
            if (queue.isClosed.get())
                throw new IllegalStateException("Queue is closed");
            boolean ok = false;
            try {
                int cycle = queue.cycle();

                if (wire == null)
                    setCycle2(cycle, true);
                else if (this.cycle != cycle)
                    rollCycleTo(cycle);

                int safeLength = (int) queue.overlapSize();
                ok = headerWriteStrategy.onContextOpen(metaData, safeLength);
                return context;

            } finally {
                assert ok || resetAppendingThread();
            }
        }

        private int handleRoll(int cycle) {
            assert !((AbstractWire) wire).isInsideHeader();
            int qCycle = queue.cycle();
            if (cycle < queue.cycle()) {
                setCycle2(cycle = qCycle, true);
            } else if (cycle == qCycle) {
                // for the rare case where the qCycle has just changed in the last
                // few milliseconds since
                setCycle2(++cycle, true);
            } else {
                throw new IllegalStateException("Found an EOF on the next cycle file," +
                        " this next file, should not have an EOF as its cycle " +
                        "number is greater than the current cycle (based on the " +
                        "current time), this should only happen " +
                        "if it was written by a different appender set with a different " +
                        "EPOCH or different roll cycle." +
                        "All your appenders ( that write to a given directory ) " +
                        "should have the same EPOCH and roll cycle" +
                        " qCycle=" + qCycle + ", cycle=" + cycle + ", queue-file=" + queue.file().getAbsolutePath());
            }
            return cycle;
        }

        boolean checkWritePositionHeaderNumber() {
            if (wire == null || wire.headerNumber() == Long.MIN_VALUE) return true;
            try {
                long pos1 = position;
         /*
                long posMax = store.writePosition();
                if (pos1 > posMax+4) {
                    System.out.println(queue.dump());
                    String message = "########### " +
                            "thread: " + Thread.currentThread().getName() +
                            "  pos1: " + pos1 +
                            " posMax: " + posMax;
                    System.err.println(message);
                    throw new AssertionError(message);
                }*/
                long seq1 = queue.rollCycle().toSequenceNumber(wire.headerNumber() + 1) - 1;
                long seq2 = store.sequenceForPosition(this, pos1, true);

                if (seq1 != seq2) {
//                    System.out.println(queue.dump());
                    String message = "~~~~~~~~~~~~~~ " +
                            "thread: " + Thread.currentThread().getName() +
                            "  pos1: " + pos1 +
                            " seq1: " + seq1 +
                            " seq2: " + seq2;
                    System.err.println(message);
                    throw new AssertionError(message);
                }

            } catch (IOException e) {
                Jvm.fatal().on(getClass(), e);
                throw Jvm.rethrow(e);
            }
            return true;
        }

        @NotNull
        @Override
        public DocumentContext writingDocument(long index) {
            context.isClosed = false;
            assert checkAppendingThread();
            context.wire = acquireBufferWire();
            context.wire.headerNumber(index);
            context.isClosed = false;
            return context;
        }

        @Override
        public int sourceId() {
            return queue.sourceId;
        }

        @Override
        public void writeBytes(@NotNull BytesStore bytes) throws UnrecoverableTimeoutException {
            append((m, w) -> w.bytes().write(m), bytes);
        }

        @NotNull
        Wire acquireBufferWire() {
            if (bufferWire == null) {
                bufferWire = queue.wireType().apply(Bytes.elasticByteBuffer());
                closableResources.bufferWireReference = bufferWire.bytes();

            } else {
                bufferWire.clear();
            }
            return bufferWire;
        }


        public void writeBytes(long index, @NotNull BytesStore bytes) {

            if (queue.isClosed.get())
                throw new IllegalStateException("Queue is closed");

            int cycle = queue.rollCycle().toCycle(index);

            if (wire == null)
                setCycle2(cycle, true);
            else if (this.cycle != cycle)
                rollCycleTo(cycle);

            int safeLength = (int) queue.overlapSize();

            assert checkAppendingThread();

            try {

                long pos = wire.bytes().writePosition();

                headerWriteStrategy.onContextOpen(false, safeLength);

                boolean rollbackDontClose = index != wire.headerNumber() + 1;
                if (rollbackDontClose) {
                    wire.bytes().writeSkip(-4);
                    wire.bytes().writeVolatileInt(wire.bytes().writePosition(), 0);
                    wire.bytes().writeLimit(wire.bytes().capacity());

                    this.position = pos;
                    ((AbstractWire) wire).forceNotInsideHeader();
                    if (index > wire.headerNumber() + 1)
                        throw new IllegalStateException("Unable to move to index " + Long.toHexString(index) + " beyond the end of the queue");
                    return;
                }

                try {
                    context.wire().bytes().write(bytes);
                } finally {
                    context.close();
                }


            } finally {
                this.appendingThread = null;
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
                setCycle2(cycle, true);
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

        /**
         * overwritten in delta wire
         *
         * @param wire
         * @param index
         */
        void beforeAppend(Wire wire, long index) {
        }

        private <T> void append(@NotNull WireWriter<T> wireWriter, T writer) throws
                UnrecoverableTimeoutException {

            assert checkAppendingThread();
            try {
                int cycle = queue.cycle();
                if (this.cycle != cycle || wire == null)
                    rollCycleTo(cycle);

                try {
                    position(store.writeHeader(wire, (int) queue.overlapSize(), timeoutMS()));
                    assert ((AbstractWire) wire).isInsideHeader();
                    beforeAppend(wire, wire.headerNumber() + 1);
                    wireWriter.write(writer, wire);
                    wire.updateHeader(position, false);
                    lastIndex(wire.headerNumber());
                    lastPosition = position;
                    lastCycle = cycle;
                    store.writePosition(position);
                    writeIndexForPosition(lastIndex, position);
                } catch (EOFException theySeeMeRolling) {
                    try {
                        append2(wireWriter, writer);
                    } catch (EOFException e) {
                        throw new AssertionError(e);
                    }
                }
            } catch (StreamCorruptedException e) {
                throw new AssertionError(e);
            } finally {
                assert resetAppendingThread();
            }
        }

        private void rollCycleTo(int cycle) throws UnrecoverableTimeoutException {
            if (wire != null) {
                // only a valid check if the wire was set.
                if (this.cycle == cycle)
                    throw new AssertionError();
                store.writeEOF(wire, timeoutMS());
            }
            setCycle2(cycle, true);
        }

        /**
         * Write an EOF marker on the current cycle if it is about to roll. It would do this any way
         * if a new message was written, but this doesn't create a new cycle or add a message.
         */
        public void writeEndOfCycleIfRequired() {
            if (wire != null && queue.cycle() != cycle) {
                store.writeEOF(wire, timeoutMS());
            }
        }

        <T> void append2(@NotNull WireWriter<T> wireWriter, T writer) throws
                UnrecoverableTimeoutException, EOFException, StreamCorruptedException {
            setCycle(Math.max(queue.cycle(), cycle + 1));
            position(store.writeHeader(wire, (int) queue.overlapSize(), timeoutMS()));
            beforeAppend(wire, wire.headerNumber() + 1);
            wireWriter.write(writer, wire);
            wire.updateHeader(position, false);
        }

        private boolean checkAppendingThread() {
            Thread appendingThread = this.appendingThread;
            Thread currentThread = Thread.currentThread();
            if (appendingThread != null) {
                if (appendingThread == currentThread)
                    throw new IllegalStateException("Nested blocks of writingDocument() not supported");
                throw new IllegalStateException("Attempting to use Appender in " + currentThread + " while used by " + appendingThread);
            }
            this.appendingThread = currentThread;
            return true;
        }

        private boolean resetAppendingThread() {
            if (this.appendingThread == null)
                throw new IllegalStateException("Attempting to release Appender in " + Thread.currentThread() + " but already released");
            this.appendingThread = null;
            return true;
        }

        void writeIndexForPosition(long index, long position)
                throws UnrecoverableTimeoutException, StreamCorruptedException {

            if (!lazyIndexing) {
                long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
                store.setPositionForSequenceNumber(this, sequenceNumber, position);
            }
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

                } else {
              /*      System.out.println("checked Thread=" + Thread.currentThread().getName() +
                            " pos: " + position +
                            " seq1: " + seq1);*/
                }

            } catch (@NotNull EOFException | UnrecoverableTimeoutException | StreamCorruptedException e) {
                throw new AssertionError(e);
            }
            return true;
        }

        class StoreAppenderContext implements DocumentContext {

            boolean isClosed;
            boolean deferredHeader;
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

            @NotNull
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
             * Call this if you have detected an error condition and you want the context
             * rolled back when it is closed, rather than committed
             */
            @Override
            public void rollbackOnClose() {
                this.rollbackOnClose = true;
            }

            @Override
            public void close() {

                if (isClosed) {
                    LOG.warn("Already Closed, close was called twice.");
                    return;
                }

                try {
                    final boolean interrupted = Thread.currentThread().isInterrupted();
                    if (rollbackOnClose || interrupted) {
                        if (interrupted)
                            LOG.warn("Thread is interrupted. Can't guarantee complete message, so not committing");
                        // zero out all contents...
                        for (long i = position + Wires.SPB_HEADER_SIZE; i <= wire.bytes().writePosition(); i++)
                            wire.bytes().writeByte(i, (byte) 0);
                        // ...and then the header, as if we had never been here
                        wire.bytes().writeVolatileInt(position, 0);
                        wire.bytes().writePosition(position);
                        ((AbstractWire) wire).forceNotInsideHeader();
                        return;
                    }

                    headerWriteStrategy.onContextClose();

                    if (wire == StoreAppender.this.wire) {
                        if (padToCacheAlign)
                            wire.padToCacheAlign();

                        boolean updatedHeader = false;
                        for (int i = 0; i < REPEAT_WHILE_ROLLING; i++) {
                            try {
                                wire.updateHeader(position, metaData);
                                updatedHeader = true;
                                break;
                            } catch (EOFException theySeeMeRolling) {
                                cycle = handleRoll(cycle);
                            }
                        }
                        if (!updatedHeader)
                            throw new IllegalStateException("Unable to roll to the current cycle");

                        lastPosition = position;
                        lastCycle = cycle;

                        if (!metaData) {
                            lastIndex(wire.headerNumber());
                            store.writePosition(position);
                            if (lastIndex != Long.MIN_VALUE)
                                writeIndexForPosition(lastIndex, position);
                            else
                                assert lazyIndexing || lastIndex == Long.MIN_VALUE || checkIndex(lastIndex, position);
                        }
                        assert checkWritePositionHeaderNumber();
                    } else if (wire != null) {
                        isClosed = true;
                        assert resetAppendingThread();
                        writeBytes(wire.headerNumber(), wire.bytes());
                        wire = StoreAppender.this.wire;
                    }
                } catch (@NotNull StreamCorruptedException | UnrecoverableTimeoutException e) {
                    throw new IllegalStateException(e);
                } finally {
                    assert isClosed || resetAppendingThread();
                }
            }

            @Override
            public long index() throws IORuntimeException {
                if (this.wire.headerNumber() == Long.MIN_VALUE) {
                    try {
                        long headerNumber0 = queue.rollCycle().toIndex(cycle, store
                                .sequenceForPosition(StoreAppender.this, position, false));
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

        @Override
        public String toString() {
            return "StoreAppender{" +
                    "queue=" + queue +
                    ", cycle=" + cycle +
                    ", position=" + position +
                    ", lastIndex=" + lastIndex +
                    ", lazyIndexing=" + lazyIndexing +
                    ", lastPosition=" + lastPosition +
                    ", lastCycle=" + lastCycle +
                    '}';
        }

        private interface HeaderWriteStrategy {
            void onContextClose();

            boolean onContextOpen(boolean metaData, int safeLength);
        }

        /**
         * Strategise previous behaviour
         */
        private class HeaderWriteStrategyOriginal implements HeaderWriteStrategy {
            @Override
            public boolean onContextOpen(boolean metaData, int safeLength) {
                for (int i = 0; i < REPEAT_WHILE_ROLLING; i++) {
                    try {
                        assert wire != null;
                        long pos = store.writeHeader(wire, safeLength, timeoutMS());
                        position(pos);
                        context.isClosed = false;
                        context.rollbackOnClose = false;
                        context.wire = wire; // Jvm.isDebug() ? acquireBufferWire() : wire;
                        context.padToCacheAlign = padToCacheAlignMode() != Padding.NEVER;
                        context.metaData(metaData);
                        return true;

                    } catch (EOFException theySeeMeRolling) {
                        cycle = handleRoll(cycle);
                    }
                }
                throw new IllegalStateException("Unable to roll to the current cycle");
            }

            @Override
            public void onContextClose() {
                // do nothing
            }
        }

        /**
         * Buffered writes while awaiting for other appenders to release the header #403
         */
        private class HeaderWriteStrategyDefer implements HeaderWriteStrategy {
            @Override
            public boolean onContextOpen(boolean metaData, int safeLength) {
                assert wire != null;
                long pos = store.tryWriteHeader(wire, safeLength);
                if (pos != WireOut.TRY_WRITE_HEADER_FAILED) {
                    position(pos);
                    context.wire = wire;
                    context.deferredHeader = false;
                } else {
                    context.wire = acquireBufferWire();
                    context.deferredHeader = true;
                }

                context.isClosed = false;
                context.rollbackOnClose = false;
                context.padToCacheAlign = padToCacheAlignMode() != Padding.NEVER;
                context.metaData(metaData);
                return true;
            }

            public void onContextClose() {
                if (context.deferredHeader) {
                    int safeLength = (int) queue.overlapSize();
                    assert wire != null;
                    assert wire != context.wire;
                    for (int i = 0; i < REPEAT_WHILE_ROLLING; i++) {
                        try {
                            // TODO: we should be able to write and update the header in one go
                            long pos = store.writeHeader(wire, safeLength, timeoutMS());
                            position(pos);
                            long length = context.wire.bytes().copyTo(wire.bytes());
                            wire.bytes().writePosition(length + wire.bytes().writePosition());
                            context.wire = wire;
                            return;
                        } catch (EOFException theySeeMeRolling) {
                            cycle = handleRoll(cycle);
                        }
                    }
                    throw new IllegalStateException("Unable to roll to the current cycle");
                }
            }
        }
    }

    private static final class ClosableResources {
        private final SingleChronicleQueue queue;
        private volatile Bytes wireReference = null;
        private volatile Bytes bufferWireReference = null;
        private volatile Bytes wireForIndexReference = null;
        private volatile CommonStore storeReference = null;

        ClosableResources(final SingleChronicleQueue queue) {
            this.queue = queue;
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
                queue.release(storeReference);
            }
        }
    }

// *************************************************************************
//
// TAILERS
//
// *************************************************************************

    /**
     * Tailer
     */
    public static class StoreTailer implements ExcerptTailer, SourceContext, ExcerptContext {
        static final int INDEXING_LINEAR_SCAN_THRESHOLD = 70;
        @NotNull
        private final SingleChronicleQueue queue;
        private final StoreTailerContext context = new StoreTailerContext();
        private final ClosableResources closableResources;
        long index; // index of the next read.
        @Nullable
        WireStore store;
        private int cycle;
        private long timeForNextCycle = Long.MAX_VALUE;
        private TailerDirection direction = TailerDirection.FORWARD;
        private Wire wireForIndex;
        private boolean readAfterReplicaAcknowledged;
        @NotNull
        private TailerState state = UNINITIALISED;
        private long indexAtCreation = Long.MIN_VALUE;
        private boolean readingDocumentFound = false;
        private final MoveToState moveToState = new MoveToState();
        private boolean shouldUpdateIndex = false;

        public StoreTailer(@NotNull final SingleChronicleQueue queue) {
            this.queue = queue;
            this.setCycle(Integer.MIN_VALUE);
            this.index = 0;
            queue.addCloseListener(this, StoreTailer::close);
            closableResources = new ClosableResources(queue);
            queue.ensureThatRollCycleDoesNotConflictWithExistingQueueFiles();
        }

        private static boolean isReadOnly(Bytes bytes) {
            return bytes instanceof MappedBytes &&
                    ((MappedBytes) bytes).isBackingFileReadOnly();
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
            if (direction == NONE && (index == indexAtCreation || index == 0) && !readingDocumentFound) {
                return NoDocumentContext.INSTANCE;
            }
            return readingDocument(false);
        }

        private void close() {
            final Wire wire = context.wire();
            if (wire != null) {
                wire.bytes().release();
            }
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
            return "StoreTailer{" +
                    "index sequence=" + queue.rollCycle().toSequenceNumber(index) +
                    ", index cycle=" + queue.rollCycle().toCycle(index) +
                    ", store=" + store + ", queue=" + queue + '}';
        }

        @NotNull
        @Override
        public DocumentContext readingDocument(boolean includeMetaData) {
            if (queue.isClosed.get())
                throw new IllegalStateException("Queue is closed");
            try {
                boolean next = false, tryAgain = true;
                if (state == FOUND_CYCLE) {
                    try {
                        next = inACycle(includeMetaData, true);
                        tryAgain = false;
                    } catch (EOFException eof) {
                        state = TailerState.END_OF_CYCLE;
                    }
                }
                if (tryAgain)
                    next = next0(includeMetaData);

                if (context.present(next)) {
                    context.setStart(context.wire().bytes().readPosition() - 4);
                    readingDocumentFound = true;
                    return context;
                }
                RollCycle rollCycle = queue.rollCycle();
                if (state == CYCLE_NOT_FOUND && direction == FORWARD) {
                    int firstCycle = queue.firstCycle();
                    if (rollCycle.toCycle(index) < firstCycle)
                        toStart();
                } else if (!next && state == CYCLE_NOT_FOUND && cycle != queue.cycle()) {
                    // appenders have moved on, it's possible that linearScan is hitting EOF, which is ignored
                    // since we can't find an entry at current index, indicate that we're at the end of a cycle
                    state = TailerState.END_OF_CYCLE;
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
            return NoDocumentContext.INSTANCE;
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
                            return inACycle(includeMetaData, true);
                        } catch (EOFException eof) {
                            state = TailerState.END_OF_CYCLE;
                        }
                        break;
                    }

                    case END_OF_CYCLE: {
                        long oldIndex = this.index;
                        int currentCycle = queue.rollCycle().toCycle(oldIndex);
                        long nextIndex = nextIndexWithNextAvailableCycle(currentCycle);

                        if (nextIndex != Long.MIN_VALUE) {
                            if (moveToIndexInternal(nextIndex)) {
                                state = FOUND_CYCLE;
                                continue;
                            }
                            if (state == END_OF_CYCLE)
                                continue;
                            if (cycle < queue.lastCycle()) {
                                // we have encountered an empty file without an EOF marker
                                // TODO: more work needed - I thought that the appender and/or tailer would write an EOF into this file
                                state = END_OF_CYCLE;
                                continue;
                            }
                            // We are here because we are waiting for an entry to be written to this file.
                            // Winding back to the previous cycle results in a re-initialisation of all the objects => garbage
                            int nextCycle = queue.rollCycle().toCycle(nextIndex);
                            cycle(nextCycle, false);
                            state = CYCLE_NOT_FOUND;
                        } else {
                            state = END_OF_CYCLE;
                        }
                        return false;
                    }
                    case BEYOND_START_OF_CYCLE: {
                        if (direction == FORWARD) {
                            state = UNINITIALISED;
                            continue;
                        }
                        if (direction == BACKWARD) {

                            // give the position of the last entry and
                            // flag we want to count it even though we don't know if it will be meta data or not.

                            boolean foundCycle = cycle(queue.rollCycle().toCycle(index), false);

                            if (foundCycle) {
                                long lastSequenceNumberInThisCycle = store.sequenceForPosition(this, Long.MAX_VALUE, false);
                                long nextIndex = queue.rollCycle().toIndex(this.cycle,
                                        lastSequenceNumberInThisCycle);
                                moveToIndexInternal(nextIndex);
                                state = FOUND_CYCLE;
                                continue;
                            }

                            int cycle = queue.rollCycle().toCycle(index);
                            long nextIndex = nextIndexWithNextAvailableCycle(cycle);

                            if (nextIndex != Long.MIN_VALUE) {
                                moveToIndexInternal(nextIndex);
                                state = FOUND_CYCLE;
                                continue;
                            }

                            state = BEYOND_START_OF_CYCLE;
                            return false;
                        }
                    }
                    throw new AssertionError("direction not set, direction=" + direction);

                    case CYCLE_NOT_FOUND:

                        if (index == Long.MIN_VALUE) {
                            if (this.store != null)
                                queue.release(this.store);
                            this.store = null;
                            closableResources.storeReference = null;
                            return false;
                        }

                        if (moveToIndexInternal(index)) {
                            state = FOUND_CYCLE;
                            continue;
                        } else
                            return false;

                    default:
                        throw new AssertionError("state=" + state);
                }
            }

            throw new IllegalStateException("Unable to progress to the next cycle, state=" + state);
        }

        private boolean inACycle(boolean includeMetaData, boolean first)
                throws EOFException, StreamCorruptedException {
            Wire wire = wire();
            Bytes<?> bytes = wire.bytes();
            bytes.readLimit(bytes.capacity());
            if (readAfterReplicaAcknowledged && inACycleCheckRep()) return false;

            if (direction != TailerDirection.FORWARD && inACycleNotForward()) return false;

            switch (wire.readDataHeader(includeMetaData)) {
                case NONE:
                    return inACycleNone(includeMetaData, first, bytes);

                case META_DATA:
                    context.metaData(true);
                    break;
                case DATA:
                    context.metaData(false);
                    break;
            }

            inACycleFound(bytes);
            return true;
        }

        private boolean inACycleCheckRep() {
            long lastSequenceAck = store.lastAcknowledgedIndexReplicated();
            long seq = queue.rollCycle().toSequenceNumber(index);
            if (seq > lastSequenceAck)
                return true;
            return false;
        }

        private boolean inACycleNotForward() {
            if (!moveToIndexInternal(index)) {
                try {
                    // after toEnd() call, index is past the end of the queue
                    // so try to go back one (to the last record in the queue)
                    if (!moveToIndexInternal(index - 1)) {
                        return true;
                    }
                } catch (RuntimeException e) {
                    // can happen if index goes negative
                    return true;
                }
            }
            return false;
        }

        private void inACycleFound(Bytes<?> bytes) throws StreamCorruptedException {
            indexEntry(bytes);

            context.closeReadLimit(bytes.capacity());
            wire().readAndSetLength(bytes.readPosition());
            long end = bytes.readLimit();
            context.closeReadPosition(end);
        }

        private boolean inACycleNone(boolean includeMetaData, boolean first, Bytes<?> bytes) throws EOFException, StreamCorruptedException {
            // if current time is not the current cycle, then write an EOF marker and
            // re-read from here, you may find that in the mean time an appender writes
            // another message, however the EOF marker will always be at the end.
            long now = queue.time().currentTimeMillis();
            boolean cycleChange2 = now >= timeForNextCycle;

            return first
                    && cycleChange2
                    && !isReadOnly(bytes)
                    && checkMoveToNextCycle(includeMetaData, bytes);
        }

        private void indexEntry(@NotNull Bytes<?> bytes) throws StreamCorruptedException {
            if (store.indexable(index)
                    && shouldUpdateIndex
                    && direction == TailerDirection.FORWARD
                    && !context.isMetaData())
                store.setPositionForSequenceNumber(this,
                        queue.rollCycle().toSequenceNumber(index), bytes
                                .readPosition());
        }

        private boolean checkMoveToNextCycle(boolean includeMetaData, @NotNull Bytes<?> bytes)
                throws EOFException, StreamCorruptedException {
            if (bytes.readWrite()) {
                long pos = bytes.readPosition();
                long lim = bytes.readLimit();
                long wlim = bytes.writeLimit();
                try {
                    bytes.writePosition(pos);
                    store.writeEOF(wire(), timeoutMS());
                } finally {
                    bytes.writeLimit(wlim);
                    bytes.readLimit(lim);
                    bytes.readPosition(pos);
                }
            } else {
                Jvm.debug().on(getClass(), "Unable to append EOF to ReadOnly store, skipping");
                // even though we couldn't write EOF, we still need to indicate we're at EOF to prevent looping forever
                // only do that if we waited long enough to prevent terminating too early
                long now = queue.time().currentTimeMillis();
                if (now >= timeForNextCycle + timeoutMS() * 2)
                    throw new EOFException();
            }
            return inACycle(includeMetaData, false);
        }

        private long nextIndexWithNextAvailableCycle(int cycle) {
            if (cycle == Integer.MIN_VALUE)
                throw new AssertionError("cycle == Integer.MIN_VALUE");

            long nextIndex, doubleCheck;

            // DON'T REMOVE THIS DOUBLE CHECK - ESPECIALLY WHEN USING SECONDLY THE
            // FIRST RESULT CAN DIFFER FROM THE DOUBLE CHECK, AS THE APPENDER CAN RACE WITH THE
            // TAILER
            do {

                nextIndex = nextIndexWithNextAvailableCycle0(cycle);

                if (nextIndex != Long.MIN_VALUE) {
                    int nextCycle = queue.rollCycle().toCycle(nextIndex);
                    if (nextCycle == cycle + 1) {
                        // don't do the double check if the next cycle is adjacent to the current
                        return nextIndex;
                    }
                }

                doubleCheck = nextIndexWithNextAvailableCycle0(cycle);
            } while (nextIndex != doubleCheck);

            if (nextIndex != Long.MIN_VALUE && queue.rollCycle().toCycle(nextIndex) - 1 != cycle) {

                /**
                 * lets say that you were using a roll cycle of TEST_SECONDLY
                 * and you wrote a message to the queue, if you created a tailer and read the first message,
                 * then waited around 22 seconds before writing the next message, when the tailer
                 * came to read the next message, there would be a gap of 22 cycle files
                 * that did not exist, that is what this is reporting. If you are using daily rolling,
                 * and writing every day, you should not see this message.
                 */

                LOG.debug("Rolled " + (queue
                        .rollCycle().toCycle(nextIndex) - cycle) + " " + "times to find the " +
                        "next cycle file. This can occur if your appenders have not written " +
                        "anything for a while, leaving the cycle files with a gap.");
            }

            return nextIndex;
        }

        private long nextIndexWithNextAvailableCycle0(int cycle) {
            if (cycle > queue.lastCycle() || direction == TailerDirection.NONE) {
                return Long.MIN_VALUE;
            }

            int nextCycle = cycle + direction.add();
            boolean found = cycle(nextCycle, false);
            if (found)
                return nextIndexWithinFoundCycle(nextCycle);

            try {
                int nextCycle0 = queue.nextCycle(this.cycle, direction);
                if (nextCycle0 == -1)
                    return Long.MIN_VALUE;

                return nextIndexWithinFoundCycle(nextCycle0);

            } catch (ParseException e) {
                throw new IllegalStateException(e);
            }

        }

        private long nextIndexWithinFoundCycle(int nextCycle) {
            state = FOUND_CYCLE;
            if (direction == FORWARD)
                return queue.rollCycle().toIndex(nextCycle, 0);

            if (direction == BACKWARD) {
                try {
                    long lastSequenceNumber0 = store.lastSequenceNumber(this);
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
            return this.index;
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
                if (!cycle(cycle, false))
                    return ScanResult.NOT_REACHED;
            }

            index(index);
            ScanResult scanResult = this.store.moveToIndexForRead(this, sequenceNumber);
            Bytes<?> bytes = wire().bytes();
            if (scanResult == FOUND) {
                state = FOUND_CYCLE;
                moveToState.onSuccessfulLookup(index, direction, bytes.readPosition());
                return scanResult;
            } else if (scanResult == END_OF_FILE) {
                state = END_OF_CYCLE;
                return scanResult;
            }

            bytes.readLimit(bytes.readPosition());
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
                boolean found = cycle(firstCycle, false);
                assert found || store == null;
                if (found)
                    state = FOUND_CYCLE;
            }
            index(queue.rollCycle().toIndex(cycle, 0));

            state = FOUND_CYCLE;
            if (wire() != null)
                wire().bytes().readPosition(0);
            return this;
        }

        private boolean moveToIndexInternal(final long index) {
            moveToState.indexMoveCount++;
            final ScanResult scanResult = moveToIndexResult(index);
            return scanResult == FOUND;
        }

        /**
         * gives approximately the last index, can not be relied on as the last index may have
         * changed just after this was called. For this reason, this code is not in queue as it
         * should only be an internal method
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
                assert wireStore != null;

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
            wireForIndex = readAnywhere(wireType.apply(store.bytes()));
            closableResources.wireForIndexReference = wireForIndex.bytes();
            closableResources.wireReference = wire.bytes();
            assert headerNumberCheck((AbstractWire) wireForIndex);

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
                moveToIndexInternal(index);
            }

            return this;
        }

        @Override
        @NotNull
        public RollingChronicleQueue queue() {
            return queue;
        }

        @Override
        public Runnable getCloserJob() {
            return closableResources::releaseResources;
        }

        /**
         * Can be used to manually release resources when this
         * StoreTailer is no longer used.
         */
        public void releaseResources() {
            queue.removeCloseListener(this);
            getCloserJob().run();
        }

        private void incrementIndex() {
            RollCycle rollCycle = queue.rollCycle();
            long seq = rollCycle.toSequenceNumber(this.index);
            int cycle = rollCycle.toCycle(this.index);

            seq += direction.add();
            switch (direction) {
                case NONE:
                    break;
                case FORWARD:
                    if (rollCycle.toSequenceNumber(seq) < seq) {
                        cycle(cycle + 1, false);
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
            this.index = rollCycle.toIndex(cycle, seq);

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

        // DON'T INLINE THIS METHOD, as it's used by enterprise chronicle queue
        void index(long index) {
            this.index = index;

            if (indexAtCreation == Long.MIN_VALUE) {
                indexAtCreation = index;
            }

            moveToState.reset();
        }

        private boolean cycle(final int cycle, boolean createIfAbsent) {
            if (this.cycle == cycle && state == FOUND_CYCLE)
                return true;

            WireStore nextStore = this.queue.storeForCycle(cycle, queue.epoch(), createIfAbsent);

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
            ExcerptTailer tailer = queue.createTailer()
                    .direction(BACKWARD)
                    .toEnd();
            StringBuilder sb = new StringBuilder();
            VanillaMessageHistory veh = new VanillaMessageHistory();
            veh.addSourceDetails(false);
            while (true) {
                try (DocumentContext context = tailer.readingDocument()) {
                    if (!context.isData()) {
                        toStart();
                        return this;
                    }
                    ValueIn valueIn = context.wire().readEventName(sb);
                    if (!StringUtils.isEqual("history", sb))
                        continue;
                    final Wire wire = context.wire();
                    Object parent = wire.parent();
                    try {
                        wire.parent(null);
                        valueIn.marshallable(veh);
                    } finally {
                        wire.parent(parent);
                    }
                    int i = veh.sources() - 1;
                    if (i < 0)
                        continue;
                    if (veh.sourceId(i) != this.sourceId())
                        continue;

                    long sourceIndex = veh.sourceIndex(i);
                    if (!moveToIndexInternal(sourceIndex)) {
                        final String errorMessage = String.format("Unable to move to sourceIndex %d, " +
                                "which was determined to be the last entry written to queue %s", sourceIndex, queue);
                        throw new IORuntimeException(errorMessage);
                    }
                    try (DocumentContext content = readingDocument()) {
                        if (!content.isPresent()) {
                            final String errorMessage =
                                    String.format("No readable document found at sourceIndex %d", (sourceIndex + 1));
                            throw new IORuntimeException(errorMessage);
                        }
                        // skip this message and go to the next.
                    }
                    return this;
                }
            }
        }

        @NotNull
        @Override
        public ExcerptTailer indexing(final boolean indexing) {
            this.shouldUpdateIndex = indexing;
            return this;
        }

        public void lastAcknowledgedIndexReplicated(long acknowledgeIndex) {

            Jvm.debug().on(getClass(), "received lastAcknowledgedIndexReplicated=" + Long.toHexString(acknowledgeIndex) + " ,file=" + queue().file().getAbsolutePath());

            // the reason that we use the temp tailer is to prevent this tailer from having its cycle changed
            StoreTailer temp = queue.acquireTailer();
            try {
                RollCycle rollCycle = queue.rollCycle();
                int cycle0 = rollCycle.toCycle(acknowledgeIndex);

                if (!temp.cycle(cycle0, false)) {
                    Jvm.warn().on(getClass(), "Got an acknowledge index " + Long.toHexString(acknowledgeIndex) + " for a cycle which could not found");
                    return;
                }

                WireStore store = temp.store;
                if (store == null) {
                    Jvm.warn().on(getClass(), "Got an acknowledge index " + Long.toHexString(acknowledgeIndex) + " discarded.");
                    return;
                }
                store.lastAcknowledgedIndexReplicated(acknowledgeIndex);
            } finally {
                temp.release();
            }
        }


        public void lastIndexReplicated(long lastIndexReplicated) {

            Jvm.debug().on(getClass(), "received lastIndexReplicated=" + Long.toHexString(lastIndexReplicated) + " ,file=" + queue().file().getAbsolutePath());

            // the reason that we use the temp tailer is to prevent this tailer from having its cycle changed
            StoreTailer temp = queue.acquireTailer();
            try {
                RollCycle rollCycle = queue.rollCycle();
                int cycle0 = rollCycle.toCycle(lastIndexReplicated);

                if (!temp.cycle(cycle0, false)) {
                    Jvm.warn().on(getClass(), "Got an acknowledge index " + Long.toHexString(lastIndexReplicated) + " for a cycle which could not found");
                    return;
                }

                if (temp.store == null) {
                    Jvm.warn().on(getClass(), "Got an acknowledge index " + Long.toHexString(lastIndexReplicated) + " discarded.");
                    return;
                }
                temp.store.lastIndexReplicated(lastIndexReplicated);
            } finally {
                temp.release();
            }
        }


        public long lastAcknowledgedIndexReplicated()  {
            return ((StoreAppender) queue.acquireAppender()).store.lastAcknowledgedIndexReplicated();
        }

        public long lastIndexReplicated() {
            return ((StoreAppender) queue.acquireAppender()).store.lastIndexReplicated();
        }

        public void setCycle(int cycle) {
            this.cycle = cycle;

            timeForNextCycle = cycle == Integer.MIN_VALUE ? Long.MAX_VALUE :
                    (long) (cycle + 1) * queue.rollCycle().length() + queue.epoch();

        }

        // visible for testing
        int getIndexMoveCount() {
            return moveToState.indexMoveCount;
        }

        class StoreTailerContext extends BinaryReadDocumentContext {

            boolean rollbackOnClose = false;

            @Override
            public void rollbackOnClose() {
                rollbackOnClose = true;
            }

            StoreTailerContext() {
                super(null);
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
                    if (!rollbackOnClose && isPresent()) {
                        incrementIndex();
                        super.close();
                        // assert wire == null || wire.endUse();
                    } else {
                        present = false;
                        if (start != -1)
                            wire.bytes().readPosition(start).readLimit(readLimit);
                        start = -1;
                    }

                } finally {
                    rollbackOnClose = false;
                }
            }

            boolean present(boolean present) {
                return this.present = present;
            }

            public void wire(@Nullable AbstractWire wire) {

                AbstractWire oldWire = this.wire;
                this.wire = wire;

                if (oldWire != null) {
                    releaseWireResources(oldWire);
                }
            }
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
                    final SingleChronicleQueue queue) {
                return lastMovedToIndex != Long.MIN_VALUE &&
                        index - lastMovedToIndex < INDEXING_LINEAR_SCAN_THRESHOLD &&
                        state == FOUND_CYCLE &&
                        direction == directionAtLastMoveTo &&
                        queue.rollCycle().toCycle(index) == queue.rollCycle().toCycle(lastMovedToIndex) &&
                        index > lastMovedToIndex;
            }

            private boolean canReuseLastIndexMove(
                    final long index, final TailerState state, final TailerDirection direction,
                    final SingleChronicleQueue queue, final Wire wire) {

                return ((wire == null) || wire.bytes().readPosition() == readPositionAtLastMove) &&
                        index == this.lastMovedToIndex && index != 0 && state == FOUND_CYCLE &&
                        direction == directionAtLastMoveTo &&
                        queue.rollCycle().toCycle(index) == queue.rollCycle().toCycle(lastMovedToIndex);
            }
        }
    }

    private static void releaseWireResources(final Wire wire) {
        StoreComponentReferenceHandler.queueForRelease(wire);
    }
}