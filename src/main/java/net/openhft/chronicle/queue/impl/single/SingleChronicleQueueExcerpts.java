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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.queue.TailerDirection.BACKWARD;

public class SingleChronicleQueueExcerpts {
    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueueExcerpts.class);

    @FunctionalInterface
    public interface WireWriter<T> {
        void write(
                T message,
                WireOut wireOut);
    }

    // *************************************************************************
    //
    // APPENDERS
    //
    // *************************************************************************

    /**
     * StoreAppender
     */
    public static class StoreAppender implements ExcerptAppender, ExcerptContext {
        @NotNull
        private final SingleChronicleQueue queue;
        private final StoreAppenderContext context;

        private int cycle = Integer.MIN_VALUE;
        private WireStore store;
        private Wire wire;
        private Wire bufferWire; // if you have a buffered write.
        private Wire wireForIndex;
        private long position = -1;
        private volatile Thread appendingThread = null;
        private long lastIndex = Long.MIN_VALUE;
        private boolean lazyIndexing = false;
        private long lastPosition;
        private int lastCycle;

        public StoreAppender(@NotNull SingleChronicleQueue queue) {
            this.queue = queue;
            context = new StoreAppenderContext();
        }

        @Override
        public Wire wire() {
            return wire;
        }

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
        public ExcerptAppender lazyIndexing(boolean lazyIndexing) {
            this.lazyIndexing = lazyIndexing;
            resetPosition();
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

        private void setCycle(int cycle, boolean createIfAbsent) {
            if (cycle != this.cycle)
                setCycle2(cycle, createIfAbsent);
        }

        private void setCycle2(int cycle, boolean createIfAbsent) {
            if (cycle < 0)
                throw new IllegalArgumentException("You can not have a cycle that starts " +
                        "before Epoch. cycle=" + cycle);

            SingleChronicleQueue queue = this.queue;

            if (this.store != null) {
                queue.release(this.store);
            }
            this.store = queue.storeForCycle(cycle, queue.epoch(), createIfAbsent);
            this.cycle = cycle;
            resetWires(queue);

            // only set the cycle after the wire is set.
            this.cycle = cycle;

            assert wire.startUse();
            wire.parent(this);
            wire.pauser(queue.pauserSupplier.get());
            resetPosition();
        }

        private void resetWires(SingleChronicleQueue queue) {
            WireType wireType = queue.wireType();
            wire = wireType.apply(store.bytes());
            wireForIndex = wireType.apply(store.bytes());
        }

        private void resetPosition() throws UnrecoverableTimeoutException {
            try {
                if (store == null || wire == null)
                    return;
                final long position = store.writePosition();
                wire.bytes().writePosition(position);
                if (lazyIndexing)
                    return;

                final long headerNumber = store.sequenceForPosition(this, position);
                wire.headerNumber(queue.rollCycle().toIndex(cycle, headerNumber + 1) - 1);
                checkIndex(wire.headerNumber(), wire.bytes().writePosition());
            } catch (BufferOverflowException | EOFException | StreamCorruptedException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public DocumentContext writingDocument() throws UnrecoverableTimeoutException {
            assert checkAppendingThread();
            boolean ok = false;
            try {
                for (int i = 0; i < 100; i++) {
                    try {
                        int cycle = queue.cycle();
                        if (this.cycle != cycle || wire == null) {
                            rollCycleTo(cycle);
                        }
                        assert wire != null;

                        position(
                                store.writeHeader(wire, Wires.UNKNOWN_LENGTH, timeoutMS()));

                        context.metaData = false;
                        context.wire = wire;
                        break;

                    } catch (EOFException theySeeMeRolling) {
                        // retry.
                    }
                }
                ok = true;

            } finally {
                if (!ok)
                    assert resetAppendingThread();
            }
            return context;
        }

        @Override
        public DocumentContext writingDocument(long index) {
            assert checkAppendingThread();
            context.wire = acquireBufferWire();
            context.wire.headerNumber(index);
            return context;
        }

        @Override
        public int sourceId() {
            return queue.sourceId;
        }

        @Override
        public void writeBytes(@NotNull Bytes bytes) throws UnrecoverableTimeoutException {
            // still uses append as it has a known length.
            append(Maths.toUInt31(bytes.readRemaining()), (m, w) -> w.bytes().write(m), bytes);
        }

        Wire acquireBufferWire() {
            if (bufferWire == null) {
                bufferWire = queue.wireType().apply(Bytes.elasticByteBuffer());
            } else {
                bufferWire.clear();
            }
            return bufferWire;
        }

        @Override
        public void writeBytes(long index, BytesStore bytes) throws StreamCorruptedException {
            if (index < 0)
                throw new IllegalArgumentException("index: " + index);
            if (bytes.isEmpty())
                throw new UnsupportedOperationException("Cannot append a zero length message");
            assert checkAppendingThread();
            try {
                moveToIndexForWrite(index);

                // only get the bytes after moveToIndex
                Bytes<?> wireBytes = wire.bytes();
                try {
//                    wire.bytes().writePosition(store.writePosition());
                    int length = bytes.length();
                    // sets the position
                    wire.headerNumber(index);
                    position(store.writeHeader(wire, length, timeoutMS()));
                    wireBytes.write(bytes);
                    wire.updateHeader(length, position, false);

                    writeIndexForPosition(index, position);

                    lastIndex(index);
                    lastPosition = position;
                    lastCycle = cycle;
                } catch (EOFException theySeeMeRolling) {
                    if (wireBytes.compareAndSwapInt(wireBytes.writePosition(), Wires.END_OF_DATA, Wires.NOT_COMPLETE)) {
                        wireBytes.write(bytes);
                        wire.updateHeader(0, position, false);
                    }
                }

            } catch (UnrecoverableTimeoutException | StreamCorruptedException | EOFException e) {
                throw Jvm.rethrow(e);

            } finally {
                if (wire != null) {
                    Bytes<?> wireBytes = wire.bytes();
                    store.writePosition(wireBytes.writePosition());
                    assert resetAppendingThread();
                }
            }
        }

        private void position(long position) {
            this.position = position;
        }

        // only called for writeBytes(long index, BytesStore)
        private void moveToIndexForWrite(long index) throws EOFException {
            if (wire != null && wire.headerNumber() == index)
                return;
            int cycle = queue.rollCycle().toCycle(index);

            ScanResult scanResult = moveToIndex(cycle, queue.rollCycle().toSequenceNumber(index));
            switch (scanResult) {
                case FOUND:
                    throw new IllegalStateException("Unable to move to index " + Long.toHexString(index) + " as the index already exists");
                case NOT_REACHED:
                    throw new IllegalStateException("Unable to move to index " + Long.toHexString(index) + " beyond the end of the queue");
                case NOT_FOUND:
                    break;
            }
        }

        ScanResult moveToIndex(int cycle, long sequenceNumber) throws UnrecoverableTimeoutException, EOFException {
            if (LOG.isDebugEnabled()) {
                Jvm.debug().on(getClass(), "moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
            }

            if (this.cycle != cycle) {
                if (cycle > this.cycle)
                    rollCycleTo(cycle);
                else
                    setCycle2(cycle, true);
            }

            ScanResult scanResult = this.store.moveToIndexForRead(this, sequenceNumber);
            Bytes<?> bytes = wire.bytes();
            if (scanResult == ScanResult.NOT_FOUND) {
                // so you won't read any if it ran out of data.
                bytes.writePosition(bytes.readPosition());
                return scanResult;
            }

            bytes.readLimit(bytes.readPosition());
            return scanResult;
        }

        @Override
        public long lastIndexAppended() {

            if (lastIndex != Long.MIN_VALUE)
                return lastIndex;

            if (lastPosition == Long.MIN_VALUE || wire == null) {

                throw new IllegalStateException("nothing has been appended, so there is no last index");
            }

            try {
                long sequenceNumber = store.sequenceForPosition(this, lastPosition);
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

        public SingleChronicleQueue queue() {
            return queue;
        }

        private <T> void append(int length, WireWriter<T> wireWriter, T writer) throws UnrecoverableTimeoutException {

            assert checkAppendingThread();
            try {
                int cycle = queue.cycle();
                if (this.cycle != cycle || wire == null)
                    rollCycleTo(cycle);

                try {
                    position(store.writeHeader(wire, length, timeoutMS()));
                    wireWriter.write(writer, wire);
                    wire.updateHeader(length, position, false);
                    lastIndex(wire.headerNumber());
                    lastPosition = position;
                    lastCycle = cycle;
                    store.writePosition(wire.bytes().writePosition());
                    writeIndexForPosition(lastIndex, position);
                } catch (EOFException theySeeMeRolling) {
                    try {
                        append2(length, wireWriter, writer);
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


        private long headerNumber() {
            if (wire.headerNumber() == Long.MIN_VALUE)
                try {
                    long headerNumber = store.sequenceForPosition(this, position);
                    wire.headerNumber(queue.rollCycle().toIndex(cycle, headerNumber));
                } catch (Exception e) {
                    Jvm.rethrow(e);
                }

            return wire.headerNumber();
        }

        private void rollCycleTo(int cycle) throws UnrecoverableTimeoutException {
            if (this.cycle == cycle)
                throw new AssertionError();
            if (wire != null)
                store.writeEOF(wire, timeoutMS());
            setCycle2(cycle, true);
            resetPosition();
        }

        private <T> void append2(int length, WireWriter<T> wireWriter, T writer) throws UnrecoverableTimeoutException, EOFException, StreamCorruptedException {
            setCycle(Math.max(queue.cycle(), cycle + 1), true);
            position(store.writeHeader(wire, length, timeoutMS()));

            wireWriter.write(writer, wire);
            wire.updateHeader(length, position, false);
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

        void writeIndexForPosition(long index, long position) throws UnrecoverableTimeoutException, StreamCorruptedException {
            assert lazyIndexing || checkIndex(index, position);

            if (!lazyIndexing) {
                long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
                store.setPositionForSequenceNumber(this, sequenceNumber, position);
            }
        }

        boolean checkIndex(long index, long position) {
            try {
                final long seq1 = queue.rollCycle().toSequenceNumber(index + 1) - 1;
                final long seq2 = store.sequenceForPosition(this, position);

                if (seq1 != seq2) {
                    final long seq3 = ((SingleChronicleQueueStore) store).indexing.linearScanByPosition(wireForIndex(), position, 0, 0);
                    System.out.println(Long.toHexString(seq1) + " - " + Long.toHexString(seq2) +
                            " - " + Long.toHexString(seq3));

                    System.out.println(store.dump());

                    if (seq1 != seq3) {
                        store.sequenceForPosition(this, position);
                    }
                    assert seq1 == seq3 : "seq1=" + seq1 + ", seq3=" + seq3;
                    assert seq1 == seq2 : "seq1=" + seq1 + ", seq2=" + seq2;

                } else
                    System.out.println("checked Thread=" + Thread.currentThread().getName() + " " +
                            "upto seq1=" + seq1);

            } catch (EOFException | UnrecoverableTimeoutException | StreamCorruptedException e) {
                throw new AssertionError(e);
            }
            return true;
        }

        class StoreAppenderContext implements DocumentContext {

            private boolean metaData = false;
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
            public void close() {
                boolean isClosed = false;
                try {
                    if (wire == StoreAppender.this.wire) {
                        assert wire.bytes().writePosition() >= position;
                        wire.updateHeader(position, metaData);
                        assert !((AbstractWire) wire).isInsideHeader();

                        lastIndex(wire.headerNumber());
                        lastPosition = position;
                        lastCycle = cycle;

                        if (!metaData && lastIndex != Long.MIN_VALUE)
                            writeIndexForPosition(lastIndex, position);
                        else
                            assert lazyIndexing || checkIndex(lastIndex, position);

                        store.writePosition(wire.bytes().writePosition());

                    } else {
                        isClosed = true;
                        assert resetAppendingThread();
                        writeBytes(wire.headerNumber(), wire.bytes());
                        wire = StoreAppender.this.wire;
                    }
                } catch (BufferUnderflowException bue) {
                    if (!wire.bytes().isClosed())
                        throw bue;
                } catch (StreamCorruptedException | UnrecoverableTimeoutException e) {
                    throw new IllegalStateException(e);
                } finally {
                    assert isClosed || resetAppendingThread();
                }
            }

            @Override
            public long index() throws IORuntimeException {
                if (wire.headerNumber() == Long.MIN_VALUE) {
                    try {
                        long headerNumber0 = queue.rollCycle().toIndex(cycle, store
                                .sequenceForPosition(StoreAppender.this, position));
                        assert (((AbstractWire) wire).isInsideHeader());
                        wire.headerNumber(headerNumber0 - 1);
                    } catch (IOException e) {
                        throw new IORuntimeException(e);
                    }
                }

                return isMetaData() ? wire.headerNumber() : wire.headerNumber() + 1;
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

    /**
     * Tailer
     */
    public static class StoreTailer implements ExcerptTailer, SourceContext, ExcerptContext {
        @NotNull
        private final SingleChronicleQueue queue;
        private final StoreTailerContext context = new StoreTailerContext();
        private int cycle;
        private long index; // index of the next read.
        private WireStore store;
        private TailerDirection direction = TailerDirection.FORWARD;
        private boolean lazyIndexing = false;
        private int indexSpacingMask;
        private Wire wireForIndex;

        public StoreTailer(@NotNull final SingleChronicleQueue queue) {
            this.queue = queue;
            this.cycle = Integer.MIN_VALUE;
            this.index = 0;
            indexSpacingMask = queue.rollCycle().defaultIndexSpacing() - 1;
            toStart();
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
        public ExcerptTailer lazyIndexing(boolean lazyIndexing) {
            this.lazyIndexing = lazyIndexing;
            return this;
        }

        @Override
        public boolean lazyIndexing() {
            return lazyIndexing;
        }

        @Override
        public int sourceId() {
            return queue.sourceId;
        }

        @Override
        public String toString() {
            return "StoreTailer{" +
                    "index sequence=" + queue.rollCycle().toSequenceNumber(index) +
                    ", index cycle=" + queue.rollCycle().toCycle(index) +
                    ", store=" + store + ", queue=" + queue + '}';
        }

        @Override
        public DocumentContext readingDocument(boolean includeMetaData) {
            try {
                assert wire() == null || wire().startUse();
                if (context.present(next(includeMetaData)))
                    return context;
            } catch (StreamCorruptedException e) {
                throw new IllegalStateException(e);
            } catch (UnrecoverableTimeoutException notComplete) {
                // so treat as empty.
            }
            return NoDocumentContext.INSTANCE;
        }

        private boolean next(boolean includeMetaData) throws UnrecoverableTimeoutException, StreamCorruptedException {
            if (this.store == null) { // load the first store
                final long firstIndex = queue.firstIndex();
                if (firstIndex == Long.MAX_VALUE)
                    return false;
                if (!moveToIndex(firstIndex))
                    return false;
            }
            Bytes<?> bytes = wire().bytes();
            bytes.readLimit(bytes.capacity());
            for (int i = 0; i < 1000; i++) {
                try {
                    if (direction != TailerDirection.FORWARD)
                        if (!moveToIndex(index))
                            return false;

                    switch (wire().readDataHeader(includeMetaData)) {
                        case NONE:
                            return false;
                        case META_DATA:
                            context.metaData(true);
                            break;
                        case DATA:
                            context.metaData(false);
                            break;
                    }

                    if (!lazyIndexing
                            && direction == TailerDirection.FORWARD
                            && (index & indexSpacingMask) == 0
                            && !context.isMetaData())
                        store.setPositionForSequenceNumber(this,
                                queue.rollCycle().toSequenceNumber(index), bytes
                                        .readPosition());
                    context.closeReadLimit(bytes.capacity());
                    wire().readAndSetLength(bytes.readPosition());
                    long end = bytes.readLimit();
                    context.closeReadPosition(end);
                    return true;

                } catch (EOFException eof) {
                    if (cycle <= queue.lastCycle() && direction != TailerDirection.NONE)
                        if (moveToIndex(cycle + direction.add(), 0) == ScanResult.FOUND) {
                            bytes = wire().bytes();
                            continue;
                        }
                    return false;
                }
            }
            throw new IllegalStateException("Unable to progress to the next cycle");
        }

        /**
         * @return provides an index that includes the cycle number
         */
        @Override
        public long index() {
            return index;
        }

        @Override
        public int cycle() {
            return this.cycle;
        }

        @Override
        public boolean moveToIndex(final long index) {
            final ScanResult scanResult = moveToIndexResult(index);
            return scanResult == ScanResult.FOUND;
        }

        ScanResult moveToIndexResult(long index) {
            final int cycle = queue.rollCycle().toCycle(index);
            final long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
            return moveToIndex(cycle, sequenceNumber, index);
        }

        ScanResult moveToIndex(int cycle, long sequenceNumber) {
            return moveToIndex(cycle, sequenceNumber, queue.rollCycle().toIndex(cycle, sequenceNumber));
        }

        ScanResult moveToIndex(int cycle, long sequenceNumber, long index) {
            if (LOG.isDebugEnabled()) {
                Jvm.debug().on(getClass(), "moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
            }

            if (cycle != this.cycle) {
                // moves to the expected cycle
                cycle(cycle, false);
            }
            this.index = index;
            if (store == null)
                return ScanResult.NOT_REACHED;
            ScanResult scanResult = this.store.moveToIndexForRead(this, sequenceNumber);
            Bytes<?> bytes = wire().bytes();
            if (scanResult == ScanResult.FOUND) {
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
                return this;
            }
            if (firstCycle != this.cycle) {
                // moves to the expected cycle
                cycle(firstCycle, false);
            }
            index = queue.rollCycle().toIndex(cycle, 0);
            if (wire() != null)
                wire().bytes().readPosition(0);
            return this;
        }


        /**
         * gives approximately the last index, can not be relied on as the last index may have
         * changed just after this was called. For this reason, this code is not in queue as it
         * should only be and internal method
         *
         * @return the last index at the time this method was called.
         */
        private long approximateLastIndex() {
            try {
                RollCycle rollCycle = queue.rollCycle();
                final int lastCycle = queue.lastCycle();
                if (lastCycle == Integer.MIN_VALUE)
                    return rollCycle.toIndex(queue.cycle(), 0L);

                final WireStore wireStore = queue.storeForCycle(lastCycle, queue.epoch(), false);
                assert wireStore != null;

                if (this.store != wireStore) {
                    this.store = wireStore;
                    resetWires();
                }
                // give the position of the last entry and
                // flag we want to count it even though we don't know if it will be meta data or not.
                long sequenceNumber = store.sequenceForPosition(this, Long.MAX_VALUE);
                return rollCycle.toIndex(lastCycle, sequenceNumber);

            } catch (EOFException | StreamCorruptedException | UnrecoverableTimeoutException e) {
                throw new IllegalStateException(e);
            }
        }

        private void resetWires() {
            WireType wireType = queue.wireType();

            this.context.wire((AbstractWire) readAnywhere(wireType.apply(store.bytes())));
            wireForIndex = readAnywhere(wireType.apply(store.bytes()));
        }

        private Wire readAnywhere(Wire wire) {
            Bytes<?> bytes = wire.bytes();
            bytes.readLimit(bytes.capacity());
            return wire;
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() {
            long index = approximateLastIndex();
            if (direction != TailerDirection.FORWARD)
                index--;
            if (index != Long.MIN_VALUE)
                moveToIndex(index);
            return this;
        }

        @Override
        public TailerDirection direction() {
            return direction;
        }

        @Override
        public ExcerptTailer direction(TailerDirection direction) {
            this.direction = direction;
            return this;
        }

        public RollingChronicleQueue queue() {
            return queue;
        }

        private <T> boolean __read(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c) {
            if (this.store == null) {
                toStart();
                if (this.store == null) return false;
            }

            if (read0(t, c)) {
                incrementIndex();
                return true;
            }
            return false;
        }

        private void incrementIndex() {
            RollCycle rollCycle = queue.rollCycle();
            long seq = rollCycle.toSequenceNumber(this.index);
            seq += direction.add();
            if (rollCycle.toSequenceNumber(seq) < seq) {
                cycle(cycle + 1, false);
                seq = 0;
            } else if (seq < 0) {
                if (seq == -1) {
                    cycle(cycle - 1, false);
                } else {
                    // TODO FIX so we can roll back to the precious cycle.
                    throw new IllegalStateException("Winding to the previous day not supported");
                }
            }

            this.index = rollCycle.toIndex(this.cycle, seq);
        }

        private <T> boolean read0(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c) {
            final Wire wire = wire();
            Bytes<?> bytes = wire.bytes();
            bytes.readLimit(bytes.capacity());
            for (int i = 0; i < 1000; i++) {
                try {
                    if (direction != TailerDirection.FORWARD)
                        if (!moveToIndex(index))
                            return false;

                    if (wire.readDataHeader()) {
                        wire.readAndSetLength(bytes.readPosition());
                        long end = bytes.readLimit();
                        try {
                            c.accept(t, wire);
                            return true;
                        } finally {
                            bytes.readPositionUnlimited(end);
                        }
                    }
                    return false;

                } catch (EOFException eof) {
                    if (cycle <= queue.lastCycle() && direction != TailerDirection.NONE)
                        if (moveToIndex(cycle + direction.add(), 0) == ScanResult.FOUND) {
                            bytes = wire.bytes();
                            continue;
                        }
                    return false;
                }
            }
            throw new IllegalStateException("Unable to progress to the next cycle");
        }

        @NotNull
        private StoreTailer cycle(final int cycle, boolean createIfAbsent) {
            if (this.cycle != cycle) {
                if (this.store != null) {
                    this.queue.release(this.store);
                }
                this.store = this.queue.storeForCycle(cycle, queue.epoch(), createIfAbsent);
                if (store == null) {
                    context.wire(null);

                    return this;
                }
                this.cycle = cycle;
                resetWires();
                final Wire wire = wire();
                assert wire.startUse();
                try {
                    wire.parent(this);
                    wire.pauser(queue.pauserSupplier.get());
//                if (LOG.isDebugEnabled())
//                    Jvm.debug().on(getClass(), "tailer=" + ((MappedBytes) wire.bytes()).mappedFile().file().getAbsolutePath());
                } finally {
                    assert wire.endUse();
                }
            }
            return this;
        }

        @Override
        public ExcerptTailer afterLastWritten(ChronicleQueue queue) {
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
                    // skip the one we just added.
                    if (i < 0)
                        continue;

                    long sourceIndex = veh.sourceIndex(i);
                    if (!moveToIndex(sourceIndex))
                        throw new IORuntimeException("Unable to wind to index: " + sourceIndex);
                    try (DocumentContext content = readingDocument()) {
                        if (!content.isPresent())
                            throw new IORuntimeException("Unable to wind to index: " + (sourceIndex + 1));
                        // skip this message and go to the next.
                    }
                    return this;
                }
            }
        }

        @UsedViaReflection
        public void lastAcknowledgedIndexReplicated(long acknowledgeIndex) {
            if (store != null)
                store.lastAcknowledgedIndexReplicated(acknowledgeIndex);
        }

        class StoreTailerContext extends ReadDocumentContext {
            public StoreTailerContext() {
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
                if (isPresent())
                    incrementIndex();
                super.close();
            }

            public boolean present(boolean present) {
                return this.present = present;
            }

            public void wire(AbstractWire wire) {
                this.wire = wire;
            }
        }
    }
}

