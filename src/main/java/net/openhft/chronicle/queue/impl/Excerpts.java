/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.queue.ChronicleQueue.toCycle;
import static net.openhft.chronicle.queue.ChronicleQueue.toSequenceNumber;
import static net.openhft.chronicle.wire.Wires.toIntU30;

public class Excerpts {

    @FunctionalInterface
    public interface BytesConsumer {
        boolean accept(Bytes<?> bytes)
            throws InterruptedException;
    }

    @FunctionalInterface
    public interface WireWriter<T> {
        long writeOrAdvanceIfNotEmpty(
            @NotNull WireOut wireOut,
            boolean metaData,
            @NotNull T writer);
    }


    private static final Logger LOG = LoggerFactory.getLogger(Excerpts.class);


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
        private final AbstractChronicleQueue queue;
        private long index = -1;
        private Wire wire;
        private long cycle;
        private WireStore store;
        private long nextPrefetch = OS.pageSize();

        public StoreAppender(@NotNull AbstractChronicleQueue queue) {
            this.queue = queue;
            final long lastIndex = this.queue.lastIndex();
            this.cycle = (lastIndex == -1) ? queue.cycle() : toCycle(lastIndex);

            if (this.cycle < 0)
                throw new IllegalArgumentException("You can not have a cycle that starts " +
                        "before Epoch. cycle=" + cycle);

            this.store = queue.storeForCycle(this.cycle, queue.epoch());
            this.index = this.store.sequenceNumber();

            @NotNull final MappedBytes mappedBytes = store.mappedBytes();
            if (LOG.isDebugEnabled())
                LOG.debug("appender file=" + mappedBytes.mappedFile().file().getAbsolutePath());

            wire = this.queue.wireType().apply(mappedBytes);
        }

        @Override
        public long writeDocument(@NotNull WriteMarshallable writer) {
            return internalWriteBytes(WireInternal::writeWireOrAdvanceIfNotEmpty, writer);
        }

        @Override
        public long writeBytes(@NotNull Bytes bytes) {
            return internalWriteBytes(WireInternal::writeWireOrAdvanceIfNotEmpty, bytes);
        }

        @Override
        public long writeBytes(@NotNull WriteBytesMarshallable marshallable) {
            return writeDocument(w -> marshallable.writeMarshallable(w.bytes()));
        }

        @Override
        public long index() {
            if (this.index == -1) {
                throw new IllegalStateException();
            }

            return ChronicleQueue.index(cycle(), index);
        }

        @Override
        public long cycle() {
            return this.store.cycle();
        }

        public boolean consumeBytes(BytesConsumer consumer) throws InterruptedException {
            @NotNull final Bytes<?> bytes = wire.bytes();
            final long start = bytes.writePosition();

            bytes.writeInt(Wires.NOT_READY);

            if (!consumer.accept(bytes)) {
                bytes.writeSkip(-4);
                bytes.writeInt(bytes.writePosition(), 0);
                return false;
            }

            final long len = bytes.writePosition() - start - 4;

            // no data was read from the ring buffer, so we wont write any document
            // to the appender
            if (len == 0) {
                bytes.writeSkip(-4);
                bytes.writeInt(bytes.writePosition(), 0);
                return false;
            }

            bytes.writeInt(start, toIntU30(len, "Document length %,d " +
                "out of 30-bit int range."));

            store().writePosition(bytes.writePosition())
                .storeIndexLocation(wire, start, ++index);

            return true;
        }

        private <T> long internalWriteBytes(@NotNull WireWriter<T> wireWriter, @NotNull T writer) {
            WireStore store = store();
            Bytes<?> bytes = wire.bytes();

            long position = -1;

            do {
                final long readPosition = bytes.readPosition();
                final int pbeHeader = bytes.readInt(readPosition);

                boolean isMetaData = !Wires.isData(pbeHeader);// bytes.readInt(readPosition) & Wires.META_DATA) != 0;

                if (isMetaData) {
                    // If meta-data document is detected, we need to determine
                    // its type as in case of rolling meta, the underlying store
                    // needs to be refreshed
                    if(Wires.isReady(pbeHeader)) {
                        //
                        // store = store();
                        // bytes = wire.bytes();
                    } else {
                        // if not ready loop again waiting for meta-data
                        continue;
                    }
                }

                // position will be set to zero if currently being modified with
                // unknown len
                position = wireWriter.writeOrAdvanceIfNotEmpty(wire, false, writer);

                if (!isMetaData)
                    index++;

            } while (position <= 0);

            index++;

            store.writePosition(bytes.writePosition());
            store.storeIndexLocation(wire, position, index);
            return ChronicleQueue.index(store.cycle(), index);
        }

        @ForceInline
        private WireStore store() {
            if (cycle != queue.cycle()) {
                long nextCycle = queue.cycle();
                if (store != null) {
                    while (!store.appendRollMeta(wire, nextCycle)) {
                        Thread.yield();
                    }
                    queue.release(store);
                }

                this.cycle = nextCycle;
                this.store = queue.storeForCycle(cycle, queue.epoch());
                this.wire  = queue.wireType().apply(store.mappedBytes());
                this.index = store.firstSequenceNumber();
            }

            return store;
        }

        @Override
        public void prefetch() {
            long position = wire.bytes().writePosition();
            if (position < nextPrefetch)
                return;
            long prefetch = OS.mapAlign(position);

            // touch the page without modifying it.
            wire.bytes().compareAndSwapInt(prefetch, ~0, ~0);
            nextPrefetch = prefetch + OS.pageSize();
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
    public static class StoreTailer implements ExcerptTailer {
        @NotNull
        private final AbstractChronicleQueue queue;
        private Wire wire;
        private long cycle;
        private long index;
        private WireStore store;
        private long nextPrefetch = OS.pageSize();

        public StoreTailer(@NotNull final AbstractChronicleQueue queue) {
            this.queue = queue;
            this.cycle = -1;
            toStart();
        }

        @Override
        public String toString() {
            return "StoreTailer{" +
                    "index sequence=" + ChronicleQueue.toSequenceNumber(index) +
                    ", index cycle=" + ChronicleQueue.toCycle(index) +
                    ", store=" + store + ", queue=" + queue + '}';
        }

        @Override
        public boolean readDocument(@NotNull final ReadMarshallable marshaller) {
            return readAtIndex(marshaller, ReadMarshallable::readMarshallable);
        }

        @Override
        public boolean readBytes(@NotNull final Bytes using) {
            return readAtIndex(using, (t, w) -> t.write(w.bytes()));
        }

        @Override
        public boolean readBytes(@NotNull final ReadBytesMarshallable using) {
            return readAtIndex(using, (t, w) -> t.readMarshallable(w.bytes()));
        }

        private <T> boolean readAtIndex(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c) {
            final long cycle = this.cycle;
            final long index = this.index;

            if (this.store == null) {
                final long firstIndex = queue.firstIndex();
                if (index == -1)
                    return false;
                moveToIndex(firstIndex);
            }
            if (readAt(t, c)) {
                this.index = ChronicleQueue.index(cycle, toSequenceNumber(index) + 1);
                return true;
            }
            return false;
        }

        private <T> boolean readAt(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c) {

            long roll;
            for (; ; ) {
                roll = Long.MIN_VALUE;
                wire.bytes().readLimit(wire.bytes().capacity());
                while (wire.bytes().readVolatileInt(wire.bytes().readPosition()) != 0) {

                    try (@NotNull final DocumentContext documentContext = wire.readingDocument()) {

                        if (!documentContext.isPresent())
                            return false;

                        if (documentContext.isData()) {
                            c.accept(t, wire);
                            return true;
                        }

                        // In case of meta data, if we are found the "roll" meta, we returns
                        // the next cycle (negative)
                        final StringBuilder sb = Wires.acquireStringBuilder();

                        @NotNull
                        final ValueIn vi = wire.readEventName(sb);
                        if ("roll".contentEquals(sb)) {
                            roll = vi.int32();
                            break;
                        }
                    }
                }

                // we got to the end of the file and there is no roll information
                if (roll == Long.MIN_VALUE)
                    return false;

                // roll to the next file
                cycle(roll);
                if (store == null)
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
            return ChronicleQueue.index(this.cycle, this.index);
        }


        @Override
        public boolean moveToIndex(final long index) {

            if (LOG.isDebugEnabled()) {
                LOG.debug(SingleChronicleQueueStore.IndexOffset.toBinaryString(index));
                LOG.debug(SingleChronicleQueueStore.IndexOffset.toScale());
            }

            final long expectedCycle = toCycle(index);
            if (expectedCycle != cycle)
                // moves to the expected cycle
                cycle(expectedCycle);

            cycle = expectedCycle;

            @NotNull
            final Bytes<?> bytes = wire.bytes();

            final long sequenceNumber = toSequenceNumber(index);
            if (sequenceNumber == -1) {
                bytes.readPosition(0);
                this.index = ChronicleQueue.index(cycle, sequenceNumber);
                return true;
            }

            final long position = this.store.moveToIndex(wire, ChronicleQueue.toSequenceNumber(index));
            if (position == -1)
                return false;

            bytes.readPosition(position);
            bytes.readLimit(bytes.realCapacity());

            this.index = ChronicleQueue.index(cycle, sequenceNumber - 1);
            return true;
        }


        @NotNull
        @Override
        public ExcerptTailer toStart() {
            final long index = queue.firstIndex();
            if (ChronicleQueue.toSequenceNumber(index) == -1) {
                cycle(toCycle(index));
                this.wire.bytes().readPosition(0);
                return this;
            }
            if (!moveToIndex(index))
                throw new IllegalStateException("unable to move to the start, cycle=" + cycle);

            return this;
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() throws IOException {
            if (!moveToIndex(queue.lastIndex()))
                throw new IllegalStateException("unable to move to the start");
            return this;
        }


        @NotNull
        private StoreTailer cycle(final long cycle) {
            if (this.cycle != cycle) {
                if (null != this.store) {
                    this.queue.release(this.store);
                }
                this.cycle = cycle;
                this.store = this.queue.storeForCycle(cycle, queue.epoch());
                this.wire = queue.wireType().apply(store.mappedBytes());
                moveToIndex(ChronicleQueue.index(cycle, -1));
                if (LOG.isDebugEnabled())
                    LOG.debug("tailer=" + ((MappedBytes) wire.bytes()).mappedFile().file().getAbsolutePath());
            }
            return this;
        }


        @Override
        public void prefetch() {
            long position = wire.bytes().readPosition();
            if (position < nextPrefetch)
                return;
            long prefetch = OS.mapAlign(position) + OS.pageSize();
            // touch the page without modifying it.
            wire.bytes().compareAndSwapInt(prefetch, ~0, ~0);
            nextPrefetch = prefetch + OS.pageSize();
        }

    }
}

