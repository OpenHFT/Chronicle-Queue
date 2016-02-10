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
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueStore;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static net.openhft.chronicle.bytes.NativeBytesStore.nativeStoreWithFixedCapacity;
import static net.openhft.chronicle.queue.ChronicleQueue.toCycle;
import static net.openhft.chronicle.queue.ChronicleQueue.toSequenceNumber;
import static net.openhft.chronicle.wire.Wires.toIntU30;

public class Excerpts {

    @FunctionalInterface
    public interface BytesConsumer {
        boolean accept(Bytes<?> bytes)
                throws InterruptedException;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Excerpts.class);


    // *************************************************************************
    //
    // APPENDERS
    //
    // *************************************************************************

    public static abstract class DefaultAppender<T extends ChronicleQueue> implements ExcerptAppender {
        @NotNull
        final T queue;

        public DefaultAppender(@NotNull T queue) {
            this.queue = queue;
        }

        @Override
        public long writeDocument(@NotNull WriteMarshallable writer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long writeBytes(@NotNull Bytes<?> bytes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long index() {
            throw new UnsupportedOperationException();
        }

        public abstract long cycle();

        @NotNull
        public ChronicleQueue queue() {
            return this.queue;
        }
    }

    /**
     * Unlike the other appenders the write methods are not able to return the index that he exceprt
     * was written to, as the write is deferred  using a ring buffer , and later written using a
     * background thread
     *
     * @author Rob Austin.
     */
    public static class BufferedAppender implements ExcerptAppender {

        @NotNull
        private final BytesRingBuffer ringBuffer;
        @NotNull
        private final StoreAppender underlyingAppender;
        private final Wire tempWire;
        @NotNull
        private final EventLoop eventLoop;

        public BufferedAppender(@NotNull final EventLoop eventLoop,
                                @NotNull final StoreAppender underlyingAppender,
                                final long ringBufferCapacity,
                                @NotNull final Consumer<BytesRingBufferStats> ringBufferStats) {

            this.eventLoop = eventLoop;
            this.ringBuffer = BytesRingBuffer.newInstance(nativeStoreWithFixedCapacity(
                    ringBufferCapacity));

            this.underlyingAppender = underlyingAppender;
            this.tempWire = underlyingAppender.queue().wireType().apply(Bytes.elasticByteBuffer());

            @NotNull
            final EventHandler handler = () -> {

                @NotNull final Wire wire = underlyingAppender.wire();
                @NotNull final Bytes<?> bytes = wire.bytes();
                final long start = bytes.writePosition();

                bytes.writeInt(Wires.NOT_READY);

                try {
                    if (!ringBuffer.read(bytes)) {
                        bytes.writeSkip(-4);
                        bytes.writeInt(bytes.writePosition(), 0);
                        return false;
                    }

                    final long len = bytes.writePosition() - start - 4;

                    // no data was read from the ring buffer, so we wont write any docuement to
                    // the appender
                    if (len == 0) {
                        bytes.writeSkip(-4);
                        bytes.writeInt(bytes.writePosition(), 0);
                        return false;
                    }

                    bytes.writeInt(start, toIntU30(len, "Document length %,d " +
                            "out of 30-bit int range."));

                    underlyingAppender.index++;
                    underlyingAppender.store().writePosition(wire.bytes().writePosition());
                    underlyingAppender.store().storeIndexLocation(wire, start,
                            underlyingAppender.index);

                    return true;

                } catch (Throwable t) {
                    throw Jvm.rethrow(t);
                }

            };

            eventLoop.addHandler(handler);

            eventLoop.addHandler(new EventHandler() {
                @Override
                public boolean action() throws InvalidEventHandlerException {
                    ringBufferStats.accept(ringBuffer);
                    return true;
                }

                @NotNull
                @Override
                public HandlerPriority priority() {
                    return HandlerPriority.MONITOR;
                }
            });

            eventLoop.start();

        }

        @NotNull
        public BytesRingBuffer ringBuffer() {
            return ringBuffer;
        }

        /**
         * for the best performance use net.openhft.chronicle.queue.impl.Excerpts.BufferedAppender#writeBytes(net.openhft.chronicle.bytes.Bytes)
         *
         * @param writer to write to excerpt.
         * @return always returns -1 when using the buffered appender
         */
        @Override
        public long writeDocument(@NotNull WriteMarshallable writer) {
            @NotNull final Bytes<?> bytes = tempWire.bytes();
            bytes.clear();
            writer.writeMarshallable(tempWire);
            return writeBytes(bytes);
        }

        /**
         * for the best performacne use net.openhft.chronicle.queue.impl.Excerpts.BufferedAppender#writeBytes(net.openhft.chronicle.bytes.Bytes)
         *
         * @param marshallable to write to excerpt.
         * @return always returns -1 when using the buffered appender
         */
        @Override
        public long writeBytes(@NotNull WriteBytesMarshallable marshallable) {
            @NotNull final Bytes<?> bytes = tempWire.bytes();
            bytes.clear();
            marshallable.writeMarshallable(bytes);
            return writeBytes(bytes);
        }

        /**
         * for the best performance call this method, rather than net.openhft.chronicle.queue.impl.Excerpts.BufferedAppender#writeBytes(net.openhft.chronicle.bytes.WriteBytesMarshallable)
         * or net.openhft.chronicle.queue.impl.Excerpts.BufferedAppender#writeDocument(net.openhft.chronicle.wire.WriteMarshallable)
         *
         * @param bytes to write to excerpt.
         * @return always returns -1 when using the buffered appender
         */
        @Override
        public long writeBytes(@NotNull Bytes<?> bytes) {
            try {
                while (!ringBuffer.offer(bytes))
                    Thread.yield();
                eventLoop.unpause();
            } catch (InterruptedException e) {
                throw Jvm.rethrow(e);
            }

            return -1;
        }

        @Override
        public long index() {
            throw new UnsupportedOperationException("");
        }

        @Override
        public long cycle() {
            return underlyingAppender.cycle();
        }

        @Override
        public void prefetch() {
        }
    }

    /**
     * StoreAppender
     */
    public static class StoreAppender extends DefaultAppender<AbstractChronicleQueue> {
        private long index = -1;
        private Wire wire;
        private long cycle;
        private WireStore store;
        private long nextPrefetch = OS.pageSize();

        public StoreAppender(@NotNull AbstractChronicleQueue queue) {
            super(queue);

            final long lastIndex = super.queue.lastIndex();
            this.cycle = (lastIndex == -1) ? queue.cycle() : toCycle(lastIndex);

            if (this.cycle < 0)
                throw new IllegalArgumentException("You can not have a cycle that starts " +
                        "before Epoch. cycle=" + cycle);

            this.store = queue.storeForCycle(this.cycle, queue.epoch());
            this.index = this.store.sequenceNumber();

            @NotNull final MappedBytes mappedBytes = store.mappedBytes();
            if (LOG.isDebugEnabled())
                LOG.debug("appender file=" + mappedBytes.mappedFile().file().getAbsolutePath());

            wire = this.queue().wireType().apply(mappedBytes);
        }

        public long writeDocument(@NotNull WriteMarshallable writer) {
            final WireStore store = store();
            long position;

            do {
                final long readPosition = wire.bytes().readPosition();
                boolean isMetaData = (wire.bytes().readInt(readPosition) & Wires.META_DATA) != 0;
                position = WireInternal.writeWireOrAdvanceIfNotEmpty(wire, false, writer);

                // this will be called if currently being modified with unknown length
                if (position == 0)
                    continue;

                if (!isMetaData)
                    this.index++;

            } while (position < 0);

            this.index++;

            store.writePosition(wire.bytes().writePosition());
            store.storeIndexLocation(wire, position, index);
            return ChronicleQueue.index(store.cycle(), index);
        }

        @Override
        public long writeBytes(@NotNull Bytes bytes) {
            final WireStore store = store();
            long position;

            do {

                final long readPosition = wire.bytes().readPosition();
                boolean isMetaData = (wire.bytes().readInt(readPosition) & Wires.META_DATA) != 0;
                position = WireInternal.writeWireOrAdvanceIfNotEmpty(wire, false, bytes);

                // this will be called if currently being modified with unknown length
                if (position == 0)
                    continue;

                if (!isMetaData)
                    this.index++;

            } while (position < 0);

            this.index++;

            store.writePosition(wire.bytes().writePosition());
            store.storeIndexLocation(wire, position, index);
            return ChronicleQueue.index(store.cycle(), index);
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

        @NotNull
        Wire wire() {
            return wire;
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

        @ForceInline
        private WireStore store() {
            if (this.cycle != queue.cycle()) {
                long nextCycle = queue.cycle();
                if (this.store != null) {
                    while (!this.store.appendRollMeta(wire, nextCycle)) {
                        Thread.yield();
                    }
                    this.queue.release(this.store);
                }

                this.cycle = nextCycle;
                this.store = queue.storeForCycle(this.cycle, queue.epoch());
                this.wire = this.queue().wireType().apply(store.mappedBytes());
            }

            return this.store;
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


    private static class TailerDocumentContext implements DocumentContext {

        private final ReadDocumentContext dc;
        private final StoreTailer storeTailer;
        private final Wire wire;

        TailerDocumentContext(Wire wire, StoreTailer storeTailer) {
            this.storeTailer = storeTailer;
            this.dc = new ReadDocumentContext(wire);
            this.wire = wire;
        }

        public void start() {
            dc.start();
        }

        @Override
        public boolean isMetaData() {
            return dc.isMetaData();
        }

        @Override
        public boolean isPresent() {
            return dc.isPresent();
        }

        @Override
        public boolean isData() {
            return dc.isData();
        }

        @Override
        public Wire wire() {
            return wire;
        }

        @Override
        public void close() {
            storeTailer.index = ChronicleQueue.index(storeTailer.cycle, toSequenceNumber(storeTailer.index) + 1);
            dc.close();
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
        private TailerDocumentContext documentContext;

        public StoreTailer(@NotNull final AbstractChronicleQueue queue) {
            this.queue = queue;
            this.cycle = -1;
            toStart();
            documentContext = new TailerDocumentContext(wire, this);
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
        public DocumentContext readingDocument() {
            next();
            documentContext.start();
            return documentContext;
        }

        public boolean next() {
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

                this.cycle(roll);
                if (this.store == null)
                    return false;
            }
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
        public ExcerptTailer toEnd() {
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

