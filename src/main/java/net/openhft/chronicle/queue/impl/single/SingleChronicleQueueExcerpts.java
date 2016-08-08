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
import net.openhft.chronicle.bytes.NoBytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.WireStore;
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

import static net.openhft.chronicle.queue.TailerDirection.BACKWARD;
import static net.openhft.chronicle.queue.TailerDirection.FORWARD;
import static net.openhft.chronicle.queue.TailerState.*;
import static net.openhft.chronicle.queue.impl.single.ScanResult.FOUND;

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
    static class StoreAppender implements ExcerptAppender, ExcerptContext {
        static final int HEAD_ROOM = 1 << 20;
        @NotNull
        private final SingleChronicleQueue queue;
        private final StoreAppenderContext context;

        private int cycle = Integer.MIN_VALUE;
        private WireStore store;
        private Wire wire;
        private Wire bufferWire; // if you have a buffered write.
        private Wire wireForIndex;
        private long position = 0;
        private volatile Thread appendingThread = null;
        private long lastIndex = Long.MIN_VALUE;
        private boolean lazyIndexing = false;
        private long lastPosition;
        private int lastCycle;
        private long lastTouchedPage = -1;
        private long lastTouchedPos = 0;
        private boolean padToCacheAlign;

        StoreAppender(@NotNull SingleChronicleQueue queue) {
            this.queue = queue;
            queue.addCloseListener(StoreAppender.this::close);
            context = new StoreAppenderContext();
        }

        private void close() {
            Wire w0 = wireForIndex;
            if (w0 != null)
                w0.bytes().release();
            Wire w = wire;
            if (w != null)
                w.bytes().release();
        }

        @Override
        public void padToCacheAlign(boolean padToCacheAlign) {
            this.padToCacheAlign = padToCacheAlign;
        }

        @Override
        public boolean padToCacheAlign() {
            return padToCacheAlign;
        }

        @Override
        public void pretouch() {
            setCycle(queue.cycle(), true);
            long pos = store.writePosition();
            MappedBytes bytes = (MappedBytes) wire.bytes();

            if (lastTouchedPage < 0) {
                lastTouchedPage = pos - pos % OS.pageSize();
                lastTouchedPos = pos;
                String message = "Reset lastTouched to " + lastTouchedPage;
                Jvm.debug().on(getClass(), message);
            } else {
                long headroom = Math.max(HEAD_ROOM, (pos - lastTouchedPos) * 4); // for the next 4 ticks.
                long last = pos + headroom;
                Thread thread = Thread.currentThread();
                for (; lastTouchedPage < last; lastTouchedPage += OS.pageSize()) {
                    if (thread.isInterrupted())
                        break;
                    bytes.compareAndSwapInt(lastTouchedPage, 0, 0);
                }
                long pos2 = store.writePosition();
                if (Jvm.isDebugEnabled(getClass())) {
                    String message = "Advanced " + (pos - lastTouchedPos) / 1024 + " KB between pretouch() and " + (pos2 - pos) / 1024 + " KB while mapping of " + headroom / 1024 + " KB.";
                    Jvm.debug().on(getClass(), message);
                }
                lastTouchedPos = pos;
            }
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
            {
                Wire oldw = this.wire;
                this.wire = wireType.apply(store.bytes());
                if (oldw != null)
                    oldw.bytes().release();
            }
            {
                Wire old = this.wireForIndex;
                this.wireForIndex = wireType.apply(store.bytes());
                if (old != null)
                    old.bytes().release();
            }

        }

        private void resetPosition() throws UnrecoverableTimeoutException {
            try {
                lastTouchedPage = -1;

                if (store == null || wire == null)
                    return;
                position(store.writePosition());

                Bytes<?> bytes = wire.bytes();
                int header = bytes.readInt(position);
                assert position == 0 || Wires.isReadyData(header);
                bytes.writePosition(position + 4 + Wires.lengthOf(header));
                if (lazyIndexing) {
                    wire.headerNumber(Long.MIN_VALUE);
                    return;
                }


                final long headerNumber = store.sequenceForPosition(this, position, true);
//                Thread.yield();
//                long headerNumber2 = store.sequenceForPosition(this, position);
//                assert headerNumber == headerNumber2;
//                System.err.println("==== " + Thread.currentThread().getName()+" pos: "+position+" hdr: "+headerNumber);
                wire.headerNumber(queue.rollCycle().toIndex(cycle, headerNumber + 1) - 1);
                assert lazyIndexing || checkIndex(wire.headerNumber(), position);

            } catch (EOFException e) {
                // todo improve this
                // you can't index a file that has an EOF marker, sequenceForPosition can do
                // indexing and  also would not want to set a headerNumber on a file with a
                // EOF, so lets just ignore this exception
                Jvm.debug().on(SingleChronicleQueue.class, e);

            } catch (BufferOverflowException | StreamCorruptedException e) {
                throw new AssertionError(e);
            }
            assert checkWritePositionHeaderNumber();
        }

        @Override
        public DocumentContext writingDocument() throws UnrecoverableTimeoutException {
            assert checkAppendingThread();
            assert checkWritePositionHeaderNumber();
            boolean ok = false;
            try {
                int cycle = queue.cycle();
                for (int i = 0; i <= 100; i++) {
                    try {
                        if (this.cycle != cycle || wire == null) {
                            rollCycleTo(cycle);
                        }
                        assert wire != null;

                        long pos = store.writeHeader(wire, Wires.UNKNOWN_LENGTH, timeoutMS());
                        position(pos);

                        context.metaData = false;
                        context.wire = wire;
                        break;

                    } catch (EOFException theySeeMeRolling) {
                        assert !((AbstractWire) wire).isInsideHeader();
                        // retry.
                    }
                    if (i == 100)
                        throw new IllegalStateException("Unable to roll to the current cycle");
                    cycle++;
                }
                ok = true;

            } finally {
                if (!ok)
                    assert resetAppendingThread();
            }
            return context;
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
            } catch (EOFException e) {

            } catch (IOException e) {
                Jvm.fatal().on(getClass(), e);
                throw Jvm.rethrow(e);
            }
            return true;
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
                    store.writePosition(position);

                } catch (EOFException theySeeMeRolling) {
                    if (wireBytes.compareAndSwapInt(wireBytes.writePosition(), Wires.END_OF_DATA, Wires.NOT_COMPLETE)) {
                        wireBytes.write(bytes);
                        wire.updateHeader(0, position, false);
                    }
                }

            } catch (IllegalStateException ise) {
                if (!ise.getMessage().contains("index already exists")) {
                    Jvm.warn().on(getClass(), "Ignoring duplicate", ise);
                    throw ise;
                }

            } catch (StreamCorruptedException | EOFException e) {
                throw Jvm.rethrow(e);

            } finally {
                if (wire != null) {
                    Bytes<?> wireBytes = wire.bytes();
                    assert resetAppendingThread();
                }
            }
        }

        private void position(long position) {
            if (position > store.writePosition() + queue.blockSize())
                throw new IllegalArgumentException("pos: " + position + ", store.writePosition()=" +
                        store.writePosition() + " queue.blockSize()=" + queue.blockSize());
            // System.err.println("----- "+Thread.currentThread().getName()+" pos: "+position);
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
                    store.writePosition(position);
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

        private void rollCycleTo(int cycle) throws UnrecoverableTimeoutException {
            if (this.cycle == cycle)
                throw new AssertionError();
            if (wire != null)
                store.writeEOF(wire, timeoutMS());
            setCycle2(cycle, true);
        }

        @Override
        public void writeEndOfCycleIfRequired() {
            if (wire != null && queue.cycle() != cycle)
                store.writeEOF(wire, timeoutMS());
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

            } catch (EOFException | UnrecoverableTimeoutException | StreamCorruptedException e) {
                throw new AssertionError(e);
            }
            return true;
        }

        class StoreAppenderContext implements DocumentContext {

            boolean isClosed;

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
            public boolean isClosed() {
                return isClosed;
            }

            @Override
            public void close() {

                try {
                    if (wire == StoreAppender.this.wire) {
                        if (padToCacheAlign)
                            wire.padToCacheAlign();

                        wire.updateHeader(position, metaData);

                        lastIndex(wire.headerNumber());
                        lastPosition = position;
                        lastCycle = cycle;

                        if (!metaData) {
                            store.writePosition(position);
                            if (lastIndex != Long.MIN_VALUE)
                                writeIndexForPosition(lastIndex, position);
                            else
                                assert lazyIndexing || checkIndex(lastIndex, position);
                        }
                        assert checkWritePositionHeaderNumber();
                    } else if (wire != null) {
                        isClosed = true;
                        assert resetAppendingThread();
                        writeBytes(wire.headerNumber(), wire.bytes());
                        wire = StoreAppender.this.wire;
                    }
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
                                .sequenceForPosition(StoreAppender.this, position, false));
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
        private boolean readAfterReplicaAcknowledged;
        private TailerState state = UNINTIALISED;

        public StoreTailer(@NotNull final SingleChronicleQueue queue) {
            this.queue = queue;
            this.cycle = Integer.MIN_VALUE;
            this.index = 0;
            queue.addCloseListener(() -> {

                Wire wireForIndex0 = wireForIndex;
                if (wireForIndex0 != null)
                    wireForIndex0.bytes().release();

                context.close();

            });
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
                if (context.present(next(includeMetaData))) {
                    assert wire().startUse();
                    return context;
                }
            } catch (StreamCorruptedException e) {
                throw new IllegalStateException(e);
            } catch (UnrecoverableTimeoutException notComplete) {
                // so treat as empty.
            }
            return NoDocumentContext.INSTANCE;
        }


        private boolean next(boolean includeMetaData) throws UnrecoverableTimeoutException, StreamCorruptedException {
            for (int i = 0; i < 1000; i++) {
                switch (state) {
                    case UNINTIALISED:
                        final long firstIndex = queue.firstIndex();
                        if (firstIndex == Long.MAX_VALUE)
                            return false;
                        if (!moveToIndex(firstIndex))
                            return false;
                        break;

                    case FOUND_CYCLE: {
                        try {
                            return inAnCycle(includeMetaData);
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
                            if (moveToIndex(nextIndex)) {
                                state = FOUND_CYCLE;
                                continue;
                            }
                        }
                        moveToIndex(oldIndex);
                        state = END_OF_CYCLE;
                        return false;
                    }
                    case BEHOND_START_OF_CYCLE: {
                        if (direction == FORWARD) {
                            state = UNINTIALISED;
                            continue;
                        }
                        if (direction == BACKWARD) {

                            // give the position of the last entry and
                            // flag we want to count it even though we don't know if it will be meta data or not.
                            try {

                                boolean foundCycle = cycle(queue.rollCycle().toCycle(index), false);

                                if (foundCycle) {
                                    long lastSequenceNumberInThisCycle = store.sequenceForPosition(this, Long.MAX_VALUE, false);
                                    long nextIndex = queue.rollCycle().toIndex(this.cycle,
                                            lastSequenceNumberInThisCycle);
                                    moveToIndex(nextIndex);
                                    state = FOUND_CYCLE;
                                    continue;
                                }


                                int cycle = queue.rollCycle().toCycle(index);
                                long nextIndex = nextIndexWithNextAvailableCycle(cycle);

                                if (nextIndex != Long.MIN_VALUE) {
                                    moveToIndex(nextIndex);
                                    state = FOUND_CYCLE;
                                    continue;
                                }

                                state = BEHOND_START_OF_CYCLE;
                                return false;

                            } catch (EOFException e) {
                                throw new AssertionError(e);
                            }

                        }
                    }
                    throw new AssertionError("direction not set, direction=" + direction);


                    case CYCLE_NOT_FOUND:

                        if (index == Long.MIN_VALUE) {
                            if (this.store != null)
                                queue.release(this.store);
                            this.store = null;
                            return false;
                        }

                        if (moveToIndex(index)) {
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


        private static boolean isReadOnly(Bytes bytes) {
            if (bytes instanceof MappedBytes)
                return !((MappedBytes) bytes).mappedFile().file().canWrite();
            else
                return false;
        }

        private boolean inAnCycle(boolean includeMetaData) throws EOFException,
                StreamCorruptedException {
            Bytes<?> bytes = wire().bytes();
            bytes.readLimit(bytes.capacity());
            if (readAfterReplicaAcknowledged) {
                long lastSequenceAck = store.lastAcknowledgedIndexReplicated();
                long seq = queue.rollCycle().toSequenceNumber(index);
                if (seq > lastSequenceAck)
                    return false;
            }

            if (direction != TailerDirection.FORWARD)
                if (!moveToIndex(index))
                    return false;
            switch (wire().readDataHeader(includeMetaData)) {
                case NONE: {

                    // if current time is not the current cycle, then write an EOF marker and
                    // re-read from here, you may find that in the mean time an appender writes
                    // another message, however the EOF marker will always be at the end.
                    if (cycle != queue.cycle() && !isReadOnly(bytes)) {
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

                        return inAnCycle(includeMetaData);
                    }

                    return false;
                }
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
        }

        private long nextIndexWithNextAvailableCycle(int cycle) {
            if (cycle == Integer.MIN_VALUE)
                throw new AssertionError("cycle == Integer.MIN_VALUE");

            long nextIndex, doubleCheck;

            // DONT REMVOVE THIS DOUBLE CHECK - ESPECIALLY WHEN USING SECONDLY THE
            // FIRST RESULT CAN DIFFER FROM THE DOUBLE CHECK, AS THE APPENDER CAN RACE WITH THE
            // TAILER
            do {

/*
                if (times != 0)
                    System.out.println("times=" + times);
                times++;*/
                nextIndex = nextIndexWithNextAvailableCycle0(cycle);
                doubleCheck = nextIndexWithNextAvailableCycle0(cycle);
            } while (nextIndex != doubleCheck);

            if (nextIndex != Long.MIN_VALUE && queue.rollCycle().toCycle(nextIndex) - 1 != cycle)
                System.out.println("rolled " + (queue.rollCycle().toCycle(nextIndex) - cycle) + " times");

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
                    long lastSequenceNumber0 = store.sequenceForPosition(this, Long
                            .MAX_VALUE, false) - 1;
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
            return index;
        }

        @Override
        public int cycle() {
            return this.cycle;
        }

        @Override
        public boolean moveToIndex(final long index) {
            final ScanResult scanResult = moveToIndexResult(index);
            return scanResult == FOUND;
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

            if (cycle != this.cycle || state != FOUND_CYCLE) {
                // moves to the expected cycle
                if (!cycle(cycle, false))
                    return ScanResult.NOT_REACHED;
            }

            this.index = index;
            ScanResult scanResult = this.store.moveToIndexForRead(this, sequenceNumber);
            Bytes<?> bytes = wire().bytes();
            if (scanResult == FOUND) {
                state = FOUND_CYCLE;
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
                state = UNINTIALISED;
                return this;
            }
            if (firstCycle != this.cycle) {
                // moves to the expected cycle
                boolean found = cycle(firstCycle, false);
                assert found || store == null;
                if (found)
                    state = FOUND_CYCLE;
            }
            index = queue.rollCycle().toIndex(cycle, 0);
            state = FOUND_CYCLE;
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
                    this.store.release();
                    this.store = wireStore;
                    this.store.reserve();
                    resetWires();
                }
                // give the position of the last entry and
                // flag we want to count it even though we don't know if it will be meta data or not.
                long sequenceNumber = store.sequenceForPosition(this, Long.MAX_VALUE, false);
                return rollCycle.toIndex(lastCycle, sequenceNumber);

            } catch (EOFException | StreamCorruptedException | UnrecoverableTimeoutException e) {
                throw new IllegalStateException(e);
            }
        }

        private void resetWires() {
            WireType wireType = queue.wireType();

            final AbstractWire wire = (AbstractWire) readAnywhere(wireType.apply(store.bytes()));
            this.context.wire(wire);

            Wire wireForIndexOld = wireForIndex;

            wireForIndex = readAnywhere(wireType.apply(store.bytes()));

            if (wireForIndexOld != null && wireForIndexOld.bytes().bytesStore() != NoBytesStore.NO_BYTES_STORE)
                wireForIndexOld.bytes().release();

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
            if (state() == TailerState.CYCLE_NOT_FOUND)
                state = UNINTIALISED;
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

        private void incrementIndex() {
            RollCycle rollCycle = queue.rollCycle();
            long seq = rollCycle.toSequenceNumber(this.index);
            int cycle = rollCycle.toCycle(this.index);

            seq += direction.add();
            if (rollCycle.toSequenceNumber(seq) < seq) {
                cycle(cycle + 1, false);
                seq = 0;
            } else if (seq < 0) {
                if (seq == -1) {
                    this.index = rollCycle.toIndex(cycle - 1, seq);
                    this.state = BEHOND_START_OF_CYCLE;
                    return;
                } else {
                    // TODO FIX so we can roll back to the precious cycle.
                    throw new IllegalStateException("Winding to the previous day not supported");
                }
            }

            this.index = rollCycle.toIndex(cycle, seq);
        }

        private boolean cycle(final int cycle, boolean createIfAbsent) {
            if (this.cycle == cycle && state == FOUND_CYCLE)
                return true;

            WireStore nextStore = this.queue.storeForCycle(cycle, queue.epoch(), createIfAbsent);

            if (nextStore == null && this.store == null)
                return false;

            if (nextStore == this.store)
                return true;

            if (nextStore == null) {
                if (direction == BACKWARD)
                    state = BEHOND_START_OF_CYCLE;
                else
                    state = CYCLE_NOT_FOUND;
                return false;
            } else {
                if (this.store != null) {
                    this.queue.release(this.store);
                }
                context.wire(null);
                if (this.store != null)
                    this.store.release();
                this.store = nextStore;
                if (this.store != null)
                    this.store.reserve();
            }


            this.state = FOUND_CYCLE;
            this.cycle = cycle;
            resetWires();
            final Wire wire = wire();

            wire.parent(this);
            wire.pauser(queue.pauserSupplier.get());
            return true;
        }

        @Override
        public void readAfterReplicaAcknowledged(boolean readAfterReplicaAcknowledged) {
            this.readAfterReplicaAcknowledged = readAfterReplicaAcknowledged;
        }

        @Override
        public boolean readAfterReplicaAcknowledged() {
            return readAfterReplicaAcknowledged;
        }

        @Override
        public TailerState state() {
            return state;
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
            RollCycle rollCycle = queue.rollCycle();
            int cycle0 = rollCycle.toCycle(acknowledgeIndex);
            if (cycle0 != cycle) {
                if (!cycle(cycle0, false)) {
                    Jvm.warn().on(getClass(), "Got an acknowledge index " + Long.toHexString(acknowledgeIndex) + " for a cycle which could not found");
                    return;
                }
            }
            if (store == null) {
                Jvm.warn().on(getClass(), "Got an acknowledge index " + Long.toHexString(acknowledgeIndex) + " discarded.");
                return;
            }
            store.lastAcknowledgedIndexReplicated(rollCycle.toSequenceNumber(acknowledgeIndex));
        }

        class StoreTailerContext extends ReadDocumentContext {
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
                if (isPresent())
                    incrementIndex();

                super.close();
                // assert wire == null || wire.endUse();
            }

            boolean present(boolean present) {
                return this.present = present;
            }

            public void wire(@Nullable AbstractWire wire) {

                if (wire != null && wire.bytes().bytesStore() != NoBytesStore.NO_BYTES_STORE)
                    wire.bytes().reserve();

                AbstractWire oldWire = this.wire;
                this.wire = wire;

                if (oldWire != null &&
                        oldWire.bytes().bytesStore() != NoBytesStore.NO_BYTES_STORE)
                    oldWire.bytes().release();
            }
        }
    }
}

