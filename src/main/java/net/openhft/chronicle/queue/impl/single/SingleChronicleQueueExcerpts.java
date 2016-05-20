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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.StreamCorruptedException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

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
    public static class StoreAppender implements ExcerptAppender {
        @NotNull
        private final SingleChronicleQueue queue;
        private final StoreAppenderContext context;
        private final int indexSpacingMask;
        private int cycle = Integer.MIN_VALUE;
        private WireStore store;
        private AbstractWire wire;
        private long position = -1;
        private volatile Thread appendingThread = null;
        private long lastIndex = Long.MIN_VALUE;
        private boolean lazyIndexing = false;

        public StoreAppender(@NotNull SingleChronicleQueue queue) {
            this.queue = queue;
            indexSpacingMask = queue.rollCycle().defaultIndexSpacing() - 1;
            context = new StoreAppenderContext();
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
            if (store == null) {
                wire = null;
                return;
            }
            this.cycle = cycle;

            wire = (AbstractWire) queue.wireType().apply(store.bytes());
            // only set the cycle after the wire is set.
            this.cycle = cycle;

            assert wire.startUse();
            wire.parent(this);
            wire.pauser(queue.pauserSupplier.get());
            resetPosition();
        }

        private void resetPosition() {
            if (store == null || wire == null)
                return;
            final long position = store.writePosition();
            wire.bytes().writePosition(position);
            if (lazyIndexing)
                return;
            try {
                final long headerNumber = store.indexForPosition(wire, position, queue.timeoutMS);
                wire.headerNumber(queue.rollCycle().toIndex(cycle, headerNumber));
            } catch (EOFException | TimeoutException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public DocumentContext writingDocument() {
            assert checkAppendingThread();
            try {
                try {
                    for (int i = 0; i < 100; i++) {
                        try {

                            int cycle = queue.cycle();
                            if (this.cycle != cycle || wire == null) {
                                rollCycleTo(cycle);
                                wire.bytes().writePosition(store.writePosition());
                            }

                            assert wire != null;
                            if (wire.bytes().writePosition() >= wire.bytes().writeLimit()) {
                                LOG.debug("Reset write position");
                                wire.bytes().writePosition(store.writePosition());
                            }
                            position = wire.writeHeader(queue.timeoutMS, TimeUnit.MILLISECONDS);
                            lastIndex = wire.headerNumber();
                            context.metaData = false;
                            break;
                        } catch (EOFException theySeeMeRolling) {
                            // retry.
                        }
                    }

                } catch (Exception e) {
                    assert resetAppendingThread();
                    throw e;
                }
            } catch (TimeoutException e) {
                throw new IllegalStateException(e);
            }
            return context;
        }

        @Override
        public int sourceId() {
            return queue.sourceId;
        }

        @Override
        public void writeBytes(@NotNull Bytes bytes) {
            // still uses append as it has a known length.
            append(Maths.toUInt31(bytes.readRemaining()), (m, w) -> w.bytes().write(m), bytes);
        }

        @Override
        public void writeBytes(long index, Bytes<?> bytes) throws StreamCorruptedException {
            if (bytes.isEmpty())
                throw new UnsupportedOperationException("Cannot append a zero length message");
            assert checkAppendingThread();
            try {
                if (index != lastIndex + 1) {
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

                // only get the bytes after moveToIndex
                Bytes<?> wireBytes = wire.bytes();
                try {
//                    wire.bytes().writePosition(store.writePosition());
                    int length = bytes.length();
                    position = wire.writeHeader(length, queue.timeoutMS, TimeUnit.MILLISECONDS);
                    wireBytes.write(bytes);
                    wire.updateHeader(length, position, false);

                } catch (EOFException theySeeMeRolling) {
                    if (wireBytes.compareAndSwapInt(wireBytes.writePosition(), Wires.END_OF_DATA, Wires.NOT_COMPLETE)) {
                        wireBytes.write(bytes);
                        wire.updateHeader(0, position, false);
                    }
                }
                lastIndex = index;

            } catch (TimeoutException | StreamCorruptedException | EOFException e) {
                throw Jvm.rethrow(e);

            } finally {
                Bytes<?> wireBytes = wire.bytes();
                store.writePosition(wireBytes.writePosition());
                assert resetAppendingThread();
            }
        }

        ScanResult moveToIndex(int cycle, long sequenceNumber) throws TimeoutException, EOFException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
            }

            if (this.cycle != cycle) {
                if (cycle > this.cycle)
                    rollCycleTo(cycle);
                else
                    setCycle2(cycle, true);
            }

            ScanResult scanResult = this.store.moveToIndex(wire, sequenceNumber, queue.timeoutMS);
            Bytes<?> bytes = wire.bytes();
            if (scanResult == ScanResult.NOT_FOUND) {
                wire.bytes().writePosition(wire.bytes().readPosition());
                return scanResult;
            }
            bytes.readLimit(bytes.readPosition());
            return scanResult;
        }

        @Override
        public long lastIndexAppended() {
            if (lastIndex != Long.MIN_VALUE)
                return lastIndex;

            if (this.position == -1)
                throw new IllegalStateException("no messages written");
            try {
                final long timeoutMS = lazyIndexing ? 0 : queue.timeoutMS;
                long sequenceNumber = store.indexForPosition(wire, position, timeoutMS);
                lastIndex = queue.rollCycle().toIndex(cycle, sequenceNumber);
                return lastIndex;
            } catch (EOFException | TimeoutException e) {
                throw new AssertionError(e);
            } finally {
                wire.bytes().writePosition(store.writePosition());
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

        private <T> void append(int length, WireWriter<T> wireWriter, T writer) {
            lastIndex = Long.MIN_VALUE;
            assert checkAppendingThread();
            try {
                int cycle = queue.cycle();
                if (this.cycle != cycle || wire == null)
                    rollCycleTo(cycle);

                try {
                    position = wire.writeHeader(length, queue.timeoutMS, TimeUnit.MILLISECONDS);
                    wireWriter.write(writer, wire);
                    lastIndex = wire.headerNumber();
                    wire.updateHeader(length, position, false);
                    writeIndexForPosition(lastIndex, position, queue.timeoutMS);

                } catch (EOFException theySeeMeRolling) {
                    append2(length, wireWriter, writer);
                }
            } catch (TimeoutException | EOFException | StreamCorruptedException e) {
                throw Jvm.rethrow(e);

            } finally {
                store.writePosition(wire.bytes().writePosition());
                assert resetAppendingThread();
            }
        }

        private void rollCycleTo(int cycle) throws TimeoutException, EOFException {
            if (this.cycle == cycle)
                throw new AssertionError();
            if (wire != null)
                wire.writeEndOfWire(queue.timeoutMS, TimeUnit.MILLISECONDS);
            setCycle2(cycle, true);
            resetPosition();
        }

        private <T> void append2(int length, WireWriter<T> wireWriter, T writer) throws TimeoutException, EOFException, StreamCorruptedException {
            setCycle(Math.max(queue.cycle(), cycle + 1), true);
            position = wire.writeHeader(length, queue.timeoutMS, TimeUnit.MILLISECONDS);
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

        void writeIndexForPosition(long index, long position, long timeoutMS) {
            assert lazyIndexing || checkIndex(index, position, timeoutMS);

            if (!lazyIndexing && (index & indexSpacingMask) == 0) {
                store.setPositionForIndex(context.wire(),
                        queue.rollCycle().toSequenceNumber(index),
                        position, timeoutMS);
            }
        }

        boolean checkIndex(long index, long position, long timeoutMS) {
            try {
                final long seq1 = queue.rollCycle().toSequenceNumber(index);
                final long seq2 = store.indexForPosition(wire, position, timeoutMS);
                assert seq1 == seq2;
            } catch (EOFException | TimeoutException e) {
                throw new AssertionError(e);
            }
            return true;
        }

        class StoreAppenderContext implements DocumentContext {

            private boolean metaData = false;

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
                try {
                    final long timeoutMS = queue.timeoutMS;
                    wire.updateHeader(position, metaData);
                    long index = wire.headerNumber() - 1;
                    long position2 = wire.bytes().writePosition();
                    store.writePosition(position2);
                    if (!metaData)
                        writeIndexForPosition(index, position, timeoutMS);

                } catch (StreamCorruptedException e) {
                    throw new IllegalStateException(e);
                } finally {
                    assert resetAppendingThread();
                }
            }

            @Override
            public long index() {
                return wire.headerNumber();
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
    public static class StoreTailer implements ExcerptTailer, SourceContext {
        @NotNull
        private final SingleChronicleQueue queue;
        private final StoreTailerContext context = new StoreTailerContext();
        private int cycle;
        private long index; // index of the next read.
        private WireStore store;
        private TailerDirection direction = TailerDirection.FORWARD;
        private boolean lazyIndexing = false;
        private int indexSpacingMask;

        public StoreTailer(@NotNull final SingleChronicleQueue queue) {
            this.queue = queue;
            this.cycle = Integer.MIN_VALUE;
            this.index = 0;
            indexSpacingMask = queue.rollCycle().defaultIndexSpacing() - 1;
            toStart();
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
                assert context.wire() == null || context.wire().startUse();
                if (context.present(next(includeMetaData)))
                    return context;
            } catch (TimeoutException ignored) {
            }
            return NoDocumentContext.INSTANCE;
        }

        private boolean next(boolean includeMetaData) throws TimeoutException {
            if (this.store == null) { // load the first store
                final long firstIndex = queue.firstIndex();
                if (firstIndex == Long.MAX_VALUE)
                    return false;
                if (!this.moveToIndex(firstIndex)) return false;
            }
            Bytes<?> bytes = context.wire().bytes();
            bytes.readLimit(bytes.capacity());
            for (int i = 0; i < 1000; i++) {
                try {
                    if (direction != TailerDirection.FORWARD)
                        try {
                            moveToIndex(index);
                        } catch (TimeoutException notComplete) {
                            return false;
                        }

                    switch (context.wire().readDataHeader(includeMetaData)) {
                        case NONE:
                            return false;
                        case META_DATA:
                            context.metaData(true);
                            break;
                        case DATA:
                            context.metaData(false);
                            break;
                    }

                    if (!lazyIndexing && direction == TailerDirection.FORWARD && (index & indexSpacingMask) == 0)
                        store.setPositionForIndex(context.wire(), queue.rollCycle().toSequenceNumber(index), bytes.readPosition(), queue.timeoutMS);
                    context.closeReadLimit(bytes.capacity());
                    context.wire().readAndSetLength(bytes.readPosition());
                    long end = bytes.readLimit();
                    context.closeReadPosition(end);
                    return true;

                } catch (EOFException eof) {
                    if (cycle <= queue.lastCycle() && direction != TailerDirection.NONE)
                        try {
                            if (moveToIndex(cycle + direction.add(), 0)) {
                                bytes = context.wire().bytes();
                                continue;
                            }
                        } catch (TimeoutException failed) {
                            // no more entries yet.
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
            if (this.store == null)
                return Long.MIN_VALUE;
            return queue.rollCycle().toIndex(this.cycle, this.index);
        }

        @Override
        public int cycle() {
            return this.cycle;
        }

        @Override
        public boolean moveToIndex(final long index) throws TimeoutException {
            final int cycle = queue.rollCycle().toCycle(index);
            final long sequenceNumber = queue.rollCycle().toSequenceNumber(index);
            return moveToIndex(cycle, sequenceNumber, index);
        }

        boolean moveToIndex(int cycle, long sequenceNumber) throws TimeoutException {
            return moveToIndex(cycle, sequenceNumber, queue.rollCycle().toIndex(cycle, sequenceNumber));
        }

        boolean moveToIndex(int cycle, long sequenceNumber, long index) throws TimeoutException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
            }

            if (cycle != this.cycle) {
                // moves to the expected cycle
                cycle(cycle, false);
            }
            this.index = index;
            if (store == null)
                return false;
            ScanResult scanResult = this.store.moveToIndex(context.wire(), sequenceNumber, queue.timeoutMS);
            Bytes<?> bytes = context.wire().bytes();
            if (scanResult == ScanResult.FOUND) {
                return true;
            }
            bytes.readLimit(bytes.readPosition());
            return false;
        }

        @NotNull
        @Override
        public final ExcerptTailer toStart() {
            assert direction != TailerDirection.BACKWARD;
            final int firstCycle = queue.firstCycle();
            if (firstCycle == Integer.MAX_VALUE) {
                return this;
            }
            if (firstCycle != this.cycle) {
                // moves to the expected cycle
                cycle(firstCycle, false);
            }
            index = queue.rollCycle().toIndex(cycle, 0);
            if (context.wire() != null)
                context.wire().bytes().readPosition(0);
            return this;
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() {
            long index = queue.nextIndexToWrite();
            if (index == Long.MIN_VALUE)
                return this;
            try {
                if (direction != TailerDirection.FORWARD &&
                        queue.rollCycle().toSequenceNumber(index + 1) != 0) {
                    index--;
                }
                moveToIndex(index);
            } catch (TimeoutException e) {
                throw new AssertionError(e);
            }
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

        private <T> boolean read(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c) {
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
            final Wire wire = context.wire();
            Bytes<?> bytes = wire.bytes();
            bytes.readLimit(bytes.capacity());
            for (int i = 0; i < 1000; i++) {
                try {
                    if (direction != TailerDirection.FORWARD)
                        try {
                            moveToIndex(index);
                        } catch (TimeoutException notComplete) {
                            return false;
                        }

                    if (wire.readDataHeader()) {
                        wire.readAndSetLength(bytes.readPosition());
                        long end = bytes.readLimit();
                        try {
                            c.accept(t, wire);
                            return true;
                        } finally {
                            bytes.readLimit(bytes.capacity()).readPosition(end);
                        }
                    }
                    return false;

                } catch (EOFException eof) {
                    if (cycle <= queue.lastCycle() && direction != TailerDirection.NONE)
                        try {
                            if (moveToIndex(cycle + direction.add(), 0)) {
                                bytes = wire.bytes();
                                continue;
                            }
                        } catch (TimeoutException failed) {
                            // no more entries yet.
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
                context.wire((AbstractWire) queue.wireType().apply(store.bytes()));
                final Wire wire = context.wire();
                assert wire.startUse();
                try {
                    wire.parent(this);
                    wire.pauser(queue.pauserSupplier.get());
//                if (LOG.isDebugEnabled())
//                    LOG.debug("tailer=" + ((MappedBytes) wire.bytes()).mappedFile().file().getAbsolutePath());
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
                    .direction(TailerDirection.BACKWARD)
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

                    try {
                        long sourceIndex = veh.sourceIndex(i);
                        if (!moveToIndex(sourceIndex))
                            throw new IORuntimeException("Unable to wind to index: " + sourceIndex);
                        try (DocumentContext content = readingDocument()) {
                            if (!content.isPresent())
                                throw new IORuntimeException("Unable to wind to index: " + (sourceIndex + 1));
                            // skip this message and go to the next.
                        }
                        return this;
                    } catch (TimeoutException e) {
                        throw new IORuntimeException(e);
                    }
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

