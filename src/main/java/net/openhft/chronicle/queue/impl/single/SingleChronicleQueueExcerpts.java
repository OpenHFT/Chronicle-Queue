/**
 * Copyright (C) 2016  higherfrequencytrading.com <p> This program is free software: you can
 * redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the License. <p> This program is
 * distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
 * General Public License for more details. <p> You should have received a copy of the GNU Lesser
 * General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.TailerDirection;
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
    public static class StoreAppender implements ExcerptAppender, DocumentContext {
        @NotNull
        private final SingleChronicleQueue queue;
        private int cycle = Integer.MIN_VALUE;
        private WireStore store;
        private Wire wire;
        private long position = -1;
        private boolean metaData = false;
        private volatile Thread appendingThread = null;

        public StoreAppender(@NotNull SingleChronicleQueue queue) {
            this.queue = queue;

            int cycle = this.queue.lastCycle();
            if (cycle < 0)
                cycle = queue.cycle();
            setCycle2(cycle);
        }

        private void setCycle(int cycle) {
            if (cycle != this.cycle)
                setCycle2(cycle);
        }

        private void setCycle2(int cycle) {
            if (cycle < 0)
                throw new IllegalArgumentException("You can not have a cycle that starts " +
                        "before Epoch. cycle=" + cycle);

            this.cycle = cycle;
            SingleChronicleQueue queue = this.queue;

            if (this.store != null) {
                queue.release(this.store);
            }
            this.store = queue.storeForCycle(cycle, queue.epoch());

            @NotNull final MappedBytes mappedBytes = store.mappedBytes();
            if (LOG.isDebugEnabled())
                LOG.debug("appender file=" + mappedBytes.mappedFile().file().getAbsolutePath());

            wire = queue.wireType().apply(mappedBytes);
            wire.pauser(queue.pauserSupplier.get());
            wire.bytes().writePosition(store.writePosition());
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
        public DocumentContext writingDocument() {
            try {
                position = wire.writeHeader(queue.timeoutMS, TimeUnit.MILLISECONDS);
                metaData = false;
            } catch (TimeoutException e) {
                throw new IllegalStateException(e);
            } catch (EOFException e) {
                throw new UnsupportedOperationException("Must roll to the next cycle");
            }
            return this;
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
                wire.updateHeader(position, metaData);
                long position = wire.bytes().writePosition();
                store.writePosition(position);
            } catch (StreamCorruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void writeDocument(@NotNull WriteMarshallable writer) {
            append(Wires.UNKNOWN_LENGTH, WriteMarshallable::writeMarshallable, writer);
        }

        @Override
        public void writeBytes(@NotNull Bytes bytes) {
            append(Maths.toUInt31(bytes.readRemaining()), (m, w) -> w.bytes().write(m), bytes);
        }

        @Override
        public void writeBytes(@NotNull WriteBytesMarshallable marshallable) {
            append(Wires.UNKNOWN_LENGTH, (m, w) -> m.writeMarshallable(w.bytes()), marshallable);
        }

        @Override
        public long lastIndexAppended() {
            if (this.position == -1)
                throw new IllegalStateException("no messages written");
            try {
                long sequenceNumber = store.indexForPosition(wire, position, queue.timeoutMS);
                final long index = queue.rollCycle().toIndex(cycle, sequenceNumber);
                return index;
            } catch (EOFException | TimeoutException e) {
                throw new AssertionError(e);
            } finally {
                wire.bytes().writePosition(store.writePosition());
            }
        }

        @Override
        public int cycle() {
            return cycle;
        }

        public SingleChronicleQueue queue() {
            return queue;
        }

        private <T> void append(int length, WireWriter<T> wireWriter, T writer) {
            assert checkAppendingThread();
            try {
                int cycle = queue.cycle();
                if (this.cycle != cycle)
                    rollCycleTo(cycle);

                try {
                    position = wire.writeHeader(length, queue.timeoutMS, TimeUnit.MILLISECONDS);
                    wireWriter.write(writer, wire);
                    wire.updateHeader(length, position, false);

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

        private void rollCycleTo(int cycle) throws TimeoutException {
            wire.writeEndOfWire(queue.timeoutMS, TimeUnit.MILLISECONDS);
            setCycle2(cycle);
        }

        private <T> void append2(int length, WireWriter<T> wireWriter, T writer) throws TimeoutException, EOFException, StreamCorruptedException {
            setCycle(queue.cycle());
            position = wire.writeHeader(length, queue.timeoutMS, TimeUnit.MILLISECONDS);
            wireWriter.write(writer, wire);
            wire.updateHeader(length, position, false);
        }

        private boolean checkAppendingThread() {
            Thread appendingThread = this.appendingThread;
            if (appendingThread != null)
                throw new IllegalStateException("Attempting to use Appneder in " + Thread.currentThread() + " while used by " + appendingThread);
            this.appendingThread = Thread.currentThread();
            return true;
        }

        private boolean resetAppendingThread() {
            if (this.appendingThread == null)
                throw new IllegalStateException("Attempting to release Appender in " + Thread.currentThread() + " but already released");
            this.appendingThread = null;
            return true;
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
    public static class StoreTailer extends ReadDocumentContext implements ExcerptTailer {
        @NotNull
        private final SingleChronicleQueue queue;
        private int cycle;
        private long index; // index of the next read.
        private WireStore store;
        private TailerDirection direction;

        public StoreTailer(@NotNull final SingleChronicleQueue queue) {
            super(null);
            this.queue = queue;
            this.cycle = Integer.MIN_VALUE;
            this.index = 0;
            toStart();
        }

        @Override
        public String toString() {
            return "StoreTailer{" +
                    "index sequence=" + queue.rollCycle().toSequenceNumber(index) +
                    ", index cycle=" + queue.rollCycle().toCycle(index) +
                    ", store=" + store + ", queue=" + queue + '}';
        }

        @Override
        public boolean readDocument(@NotNull final ReadMarshallable marshaller) {
            try {
                return read(marshaller, ReadMarshallable::readMarshallable, queue.timeoutMS);
            } catch (TimeoutException e) {
                return false;
            }
        }

        @Override
        public boolean readBytes(@NotNull final Bytes using) {
            try {
                return read(using, (t, w) -> t.write(w.bytes()), queue.timeoutMS);
            } catch (TimeoutException e) {
                return false;
            }
        }

        @Override
        public DocumentContext readingDocument() {
            boolean next;
            try {
                next = next();
            } catch (TimeoutException ignored) {
                next = false;
            }
            if (next) {
                start();
                return this;
            }
            return NoDocumentContext.INSTANCE;
        }

        @Override
        public void close() {
            if (isPresent())
                incrementIndex();
            super.close();
        }

        private boolean next() throws TimeoutException {
            if (this.store == null) { // load the first store
                final long firstIndex = queue.firstIndex();
                if (firstIndex == Long.MAX_VALUE)
                    return false;
                if (!this.moveToIndex(firstIndex)) return false;
            }
            int roll;
            for (; ; ) {
                roll = Integer.MIN_VALUE;
                final Wire wire = this.wire;
                wire.bytes().readLimit(wire.bytes().capacity());
                while (wire.bytes().readVolatileInt(wire.bytes().readPosition()) != 0) {
                    long position = wire.bytes().readPosition();
                    long limit = wire.bytes().readLimit();

                    try (@NotNull final DocumentContext documentContext = wire.readingDocument()) {
                        if (!documentContext.isPresent()) return false;
                        if (documentContext.isData()) {
                            // as we have the document, we have to roll it back to the start
                            // position in the close() method so that it can be read by the user.
                            ((ReadDocumentContext) documentContext).closeReadPosition(position);
                            ((ReadDocumentContext) documentContext).closeReadLimit(limit);
                            return true;
                        }
                        // In case of meta data, if we are found the "roll" meta, we returns
                        // the next cycle (negative)

                        final StringBuilder sb = Wires.acquireStringBuilder();
                        @NotNull final ValueIn vi = wire.readEventName(sb);
                        if ("roll".contentEquals(sb)) {
                            roll = vi.int32();
                            break;
                        }
                    }
                }

                // we got to the end of the file and there is no roll information
                if (roll == Integer.MIN_VALUE) return false; // roll to the next file

                this.moveToIndex(roll, 0);
                if (this.store == null)
                    return false;
            }
        }

        @Override
        public boolean readBytes(@NotNull final ReadBytesMarshallable using) {
            try {
                return read(using, (t, w) -> t.readMarshallable(w.bytes()), queue.timeoutMS);
            } catch (TimeoutException e) {
                return false;
            }
        }

        /**
         * @return provides an index that includes the cycle number
         */
        @Override
        public long index() {
            if (this.store == null)
                throw new IllegalArgumentException("This tailer is not bound to any cycle");
            return queue.rollCycle().toIndex(this.cycle, this.index);
        }

        @Override
        public int cycle() {
            return this.cycle;
        }

        @Override
        public boolean moveToIndex(final long index) throws TimeoutException {
            return moveToIndex(queue.rollCycle().toCycle(index), queue.rollCycle().toSequenceNumber(index), index);
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
                cycle(cycle);
            }
            this.index = index;

            ScanResult scanResult = this.store.moveToIndex(wire, sequenceNumber, queue.timeoutMS);
            Bytes<?> bytes = wire.bytes();
            if (scanResult == ScanResult.FOUND) {
                return true;
            }
            bytes.readLimit(bytes.readPosition());
            return false;
        }

        private void setPositionAtMessage(Bytes<?> bytes, long position) {
            int header = bytes.readInt(position);
            assert Wires.isReady(header);
            long limit = position + Wires.lengthOf(header) + 4;
            if (limit > bytes.readLimit()) {
                bytes.readLimit(limit);
                store.writePosition(limit);
            }
            bytes.readPosition(position);
        }

        @NotNull
        @Override
        public final ExcerptTailer toStart() {
            direction = TailerDirection.FORWARD;
            final int firstCycle = queue.firstCycle();
            if (firstCycle == Integer.MAX_VALUE) {
                return this;
            }
            if (firstCycle != this.cycle) {
                // moves to the expected cycle
                cycle(firstCycle);
            }
            index = queue.rollCycle().toIndex(cycle, 0);
            this.wire.bytes().readPosition(0);
            return this;
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() {
            // this is the last written index so the end is 1 + last written index
            long index = queue.lastIndex();
            if (index == Long.MIN_VALUE)
                return this;
            try {
                if (direction == TailerDirection.FORWARD)
                    index++;
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

        private <T> boolean read(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c, long timeoutMS) throws TimeoutException {
            if (this.store == null) {
                toStart();
                if (this.store == null) return false;
            }

            if (read0(t, c, timeoutMS)) {
                incrementIndex();
                return true;
            }
            return false;
        }

        private void incrementIndex() {
            RollCycle rollCycle = queue.rollCycle();
            long seq = rollCycle.toSequenceNumber(this.index);
            this.index = rollCycle.toIndex(this.cycle, seq + direction.add());
        }

        private <T> boolean read0(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c, long timeoutMS) {
            Bytes<?> bytes = wire.bytes();
            bytes.readLimit(bytes.capacity());
            for (int i = 0; i < 1000; i++) {
                try {
                    if (direction != TailerDirection.FORWARD)
                        try {
                            moveToIndex(index);
                        } catch (TimeoutException notReady) {
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
        private StoreTailer cycle(final int cycle) {
            if (this.cycle != cycle) {
                if (this.store != null) {
                    this.queue.release(this.store);
                }
                this.cycle = cycle;
                this.store = this.queue.storeForCycle(cycle, queue.epoch());
                this.wire = queue.wireType().apply(store.mappedBytes());
                this.wire.pauser(queue.pauserSupplier.get());
//                if (LOG.isDebugEnabled())
//                    LOG.debug("tailer=" + ((MappedBytes) wire.bytes()).mappedFile().file().getAbsolutePath());
            }
            return this;
        }

        public long lastIndex(int cycle) {
            cycle(cycle);
            long sequenceNumber = store.lastEntryIndexed(wire, queue.timeoutMS);
            return queue.rollCycle().toIndex(this.cycle, sequenceNumber);
        }
    }
}

