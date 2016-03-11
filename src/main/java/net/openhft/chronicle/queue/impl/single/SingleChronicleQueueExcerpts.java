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

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
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

import static net.openhft.chronicle.queue.impl.RollingChronicleQueue.toCycle;
import static net.openhft.chronicle.queue.impl.RollingChronicleQueue.toSequenceNumber;

public class SingleChronicleQueueExcerpts {

    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueueExcerpts.class);
    private static final boolean ASSERTIONS;
    private static final String ROLL_STRING = "roll";
    private static final int ROLL_KEY = BytesUtil.asInt(ROLL_STRING);

    static {
        boolean assertions = false;
        assert assertions = true;
        ASSERTIONS = assertions;
    }


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

            long index = this.queue.lastIndex();
            setCycleForIndex(index);
        }

        private void setCycleForIndex(long index) {
            setCycle(index == Long.MIN_VALUE ? queue.cycle() : toCycle(index));
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
            this.store = queue.storeForCycle(cycle, queue.epoch());

            @NotNull final MappedBytes mappedBytes = store.mappedBytes();
            if (LOG.isDebugEnabled())
                LOG.debug("appender file=" + mappedBytes.mappedFile().file().getAbsolutePath());

            wire = queue.wireType().apply(mappedBytes);
            wire.pauser(queue.pauserSupplier.get());
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
            return RollingChronicleQueue.index(cycle, store.indexForPosition(wire, position));
        }

        @Override
        public long cycle() {
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
        private long cycle;
        private long index; // index of the next read.
        private WireStore store;

        public StoreTailer(@NotNull final SingleChronicleQueue queue) {
            super(null);
            this.queue = queue;
            this.cycle = Long.MIN_VALUE;
            this.index = 0;
            toStart();
        }

        @Override
        public String toString() {
            return "StoreTailer{" +
                    "index sequence=" + RollingChronicleQueue.toSequenceNumber(index) +
                    ", index cycle=" + RollingChronicleQueue.toCycle(index) +
                    ", store=" + store + ", queue=" + queue + '}';
        }

        @Override
        public boolean readDocument(@NotNull final ReadMarshallable marshaller) {
            return read(marshaller, ReadMarshallable::readMarshallable);
        }

        @Override
        public boolean readBytes(@NotNull final Bytes using) {
            return read(using, (t, w) -> t.write(w.bytes()));
        }

        @Override
        public DocumentContext readingDocument() {
            if (!next())
                return NoDocumentContext.INSTANCE;
            start();

            return this;
        }

        @Override
        public void close() {
            if (isPresent())
                index = RollingChronicleQueue.index(cycle,
                        toSequenceNumber(index) + 1);
            super.close();
        }

        private boolean next() {
            if (this.store == null) { // load the first store
                final long firstIndex = queue.firstIndex();
                if (!this.moveToIndex(firstIndex)) return false;
            }
            long roll;
            for (; ; ) {
                roll = Long.MIN_VALUE;
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
                if (roll == Long.MIN_VALUE) return false; // roll to the next file

                this.moveToIndex(roll, 0);
                if (this.store == null)
                    return false;
            }
        }

        @Override
        public boolean readBytes(@NotNull final ReadBytesMarshallable using) {
            return read(using, (t, w) -> t.readMarshallable(w.bytes()));
        }

        /**
         * @return provides an index that includes the cycle number
         */
        @Override
        public long index() {
            if (this.store == null)
                throw new IllegalArgumentException("This tailer is not bound to any cycle");
            return RollingChronicleQueue.index(this.cycle, this.index);
        }

        @Override
        public long cycle() {
            return this.cycle;
        }

        @Override
        public boolean moveToIndex(final long index) {
            return moveToIndex(toCycle(index), toSequenceNumber(index), index);
        }

        boolean moveToIndex(long cycle, long sequenceNumber) {
            return moveToIndex(cycle, sequenceNumber, RollingChronicleQueue.index(cycle, sequenceNumber));
        }

        boolean moveToIndex(long cycle, long sequenceNumber, long index) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("moveToIndex: " + Long.toHexString(cycle) + " " + Long.toHexString(sequenceNumber));
            }

            if (cycle != this.cycle) {
                // moves to the expected cycle
                cycle(cycle);
            }
            this.index = index;

            @NotNull
            final Bytes<?> bytes = wire.bytes();
            final long position = this.store.moveToIndex(wire, sequenceNumber);
            if (position < 0) {
                bytes.readPosition(bytes.readLimit());
                return false;
            }
            setPositionAtMessage(bytes, position);
            return true;
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
            final int firstCycle = queue.firstCycle();
            if (firstCycle == Integer.MAX_VALUE) {
                return this;
            }
            moveToIndex(firstCycle, 0);
            this.wire.bytes().readPosition(0);
            return this;
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() {
            // this is the last written index so the end is 1 + last written index
            final long index = queue.lastIndex();
            if (index == Long.MIN_VALUE)
                return this;
            moveToIndex(index + 1);
            return this;
        }

        @Override
        public TailerDirection direction() {
            return TailerDirection.FORWARD;
        }

        @Override
        public ExcerptTailer direction(TailerDirection direction) {
            if (direction != TailerDirection.FORWARD)
                throw new UnsupportedOperationException();
            return this;
        }

        public RollingChronicleQueue queue() {
            return queue;
        }

        private <T> boolean read(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c) {
            long index = this.index;
            if (this.store == null) {
                index = queue.firstIndex();
                if (index == Long.MAX_VALUE)
                    return false;
                moveToIndex(index);
            }

            Bytes<?> bytes = wire.bytes();
            long limit = bytes.readLimit();
            try {
                bytes.readLimit(bytes.capacity());
                if (read0(t, c)) {
                    this.index = RollingChronicleQueue.index(this.cycle, toSequenceNumber(index) + 1);
                    return true;
                }
                return false;
            } finally {
                bytes.readLimit(limit);
            }
        }

        private <T> boolean read0(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c) {
            Bytes<?> bytes = wire.bytes();
            long roll;
            for (; ; ) {
                roll = Long.MIN_VALUE;

                while (bytes.readVolatileInt(bytes.readPosition()) != 0) {

                    try (@NotNull final DocumentContext documentContext = wire.readingDocument()) {

                        if (!documentContext.isPresent())
                            return false;

                        if (documentContext.isData()) {
                            c.accept(t, wire);
                            return true;
                        }

                        // In case of meta data, if we have found the "roll" meta,
                        // the next cycle (negative) is returned
                        final StringBuilder sb = Wires.acquireStringBuilder();

                        @NotNull
                        final ValueIn vi = wire.readEventName(sb);
                        if (ROLL_STRING.contentEquals(sb)) {
                            roll = vi.int32();
                            break;
                        }
                    }
                }

                // we got to the end of the file and there is no roll information
                if (roll == Long.MIN_VALUE)
                    return false;

                // roll to the next file
                moveToIndex(roll, 0);
                if (store == null)
                    return false;
            }
        }

        @NotNull
        private StoreTailer cycle(final long cycle) {
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

        public long lastIndex(long cycle) {
            cycle(cycle);
            return RollingChronicleQueue.index(this.cycle, store.lastEntryIndexed(wire));
        }
    }
}

