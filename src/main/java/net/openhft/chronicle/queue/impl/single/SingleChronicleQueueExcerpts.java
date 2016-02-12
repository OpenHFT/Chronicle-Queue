/**
 * Copyright (C) 2016  higherfrequencytrading.com
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation, either version 3
 * of the License.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.WireStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

import static net.openhft.chronicle.queue.impl.RollingChronicleQueue.toCycle;
import static net.openhft.chronicle.queue.impl.RollingChronicleQueue.toSequenceNumber;
import static net.openhft.chronicle.wire.Wires.toIntU30;

public class SingleChronicleQueueExcerpts {

    private static final Logger LOG = LoggerFactory.getLogger(SingleChronicleQueueExcerpts.class);
    private static final boolean ASSERTIONS;
    private static final String ROLL_STRING = "roll";
    private static final int ROLL_KEY = BytesUtil.asInt(ROLL_STRING);
    private static final int SPB_HEADER_SIZE = 4;

    static {
        boolean assertions = false;
        assert assertions = true;
        ASSERTIONS = assertions;
    }

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
        private long index;
        private Wire wire;
        private long cycle;
        private WireStore store;
        private long nextPrefetch;
        private AppenderDocumentContext dc;
        private volatile Thread appendingThread = null;

        public StoreAppender(@NotNull SingleChronicleQueue queue) {
            this.nextPrefetch = OS.pageSize();
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
            dc = new AppenderDocumentContext((InternalWire) wire, this);
        }

        @Override
        public DocumentContext writingDocument(boolean metaData) {
            dc.start(metaData);
            return dc;
        }

        @Override
        public long writeDocument(@NotNull WriteMarshallable writer) {
            return append(WireInternal::writeWireOrAdvanceIfNotEmpty, writer);
        }

        @Override
        public long writeBytes(@NotNull Bytes bytes) {
            return append(WireInternal::writeWireOrAdvanceIfNotEmpty, bytes);
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

            return RollingChronicleQueue.index(cycle(), index);
        }

        @Override
        public long cycle() {
            return this.store.cycle();
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

        public SingleChronicleQueue queue() {
            return queue;
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

        private <T> long append(@NotNull WireWriter<T> wireWriter, @NotNull T writer) {
            if (ASSERTIONS) {
                Thread appendingThread = this.appendingThread;
                if (appendingThread != null)
                    throw new IllegalStateException("Attempting to use Appneder in " + Thread.currentThread() + " while used by " + appendingThread);
                this.appendingThread = Thread.currentThread();
            }
            WireStore store = store();
            Bytes<?> bytes = wire.bytes();

            long position = -1;

            do {
                final long readPosition = bytes.readPosition();
                final int spbHeader = bytes.readInt(readPosition);

                if ((spbHeader & Wires.META_DATA) != 0) {
                    // If meta-data document is detected, we need to determine
                    // its type as in case of rolling meta, the underlying store
                    // needs to be refreshed
                    if (Wires.isReady(spbHeader)) {
                        if (bytes.readInt(readPosition + SPB_HEADER_SIZE) == ROLL_KEY) {
                            store = store();
                            bytes = wire.bytes();
                            bytes.writePosition(store.writePosition());
                            bytes.readPosition(store.writePosition());
                        }
                    } else {
                        // if not ready loop again waiting for meta-data being
                        // written
                        continue;
                    }
                }

                // position will be set to zero if currently being modified with
                // unknown len
                position = wireWriter.writeOrAdvanceIfNotEmpty(wire, false, writer);
            } while (position <= 0);

            index++;

            store.writePosition(bytes.writePosition());
            store.storeIndexLocation(wire, position, index);

            long index = RollingChronicleQueue.index(store.cycle(), this.index);
            if (ASSERTIONS)
                appendingThread = null;

            return index;
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
                this.wire = queue.wireType().apply(store.mappedBytes());
                this.index = store.firstSequenceNumber();
            }

            return store;
        }
    }


    private static class AppenderDocumentContext implements DocumentContext {

        private final WriteDocumentContext dc;
        private final Wire wire;
        private final StoreAppender storeAppender;

        AppenderDocumentContext(InternalWire wire, StoreAppender storeAppender) {
            this.storeAppender = storeAppender;
            this.dc = new WriteDocumentContext(wire);
            this.wire = wire;
        }

        public void start(boolean metaData) {
            dc.start(metaData);
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
            storeAppender.index++;
            dc.close();
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
            storeTailer.index = RollingChronicleQueue.index(storeTailer.cycle, toSequenceNumber(storeTailer.index) + 1);
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
            storeTailer.index = RollingChronicleQueue.index(storeTailer.cycle, toSequenceNumber(storeTailer.index) + 1);
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
        private final SingleChronicleQueue queue;
        private Wire wire;
        private long cycle;
        private long index;
        private WireStore store;
        private long nextPrefetch;
        private TailerDocumentContext dc;

        public StoreTailer(@NotNull final SingleChronicleQueue queue) {
            this.nextPrefetch = OS.pageSize();
            this.queue = queue;
            this.cycle = -1;
            this.index = -1;
            toStart();
            dc = new TailerDocumentContext(wire, this);
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
            next();
            dc.start();
            return dc;
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

                this.cycle(roll);
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("moveToIndex: " + Long.toHexString(index));
            }

            final long expectedCycle = toCycle(index);
            if (expectedCycle != cycle) {
                // moves to the expected cycle
                cycle(expectedCycle);
            }

            cycle = expectedCycle;

            @NotNull
            final Bytes<?> bytes = wire.bytes();

            final long sequenceNumber = toSequenceNumber(index);
            if (sequenceNumber == -1) {
                bytes.readPosition(0);
                this.index = RollingChronicleQueue.index(cycle, sequenceNumber);
                return true;
            }

            final long position = this.store.moveToIndex(wire, sequenceNumber);
            if (position == -1)
                return false;

            bytes.readPosition(position);
            bytes.readLimit(bytes.realCapacity());

            this.index = RollingChronicleQueue.index(cycle, sequenceNumber - 1);
            return true;
        }

        @NotNull
        @Override
        public final ExcerptTailer toStart() {
            final long index = queue.firstIndex();
            if (index != -1) {
                if (RollingChronicleQueue.toSequenceNumber(index) == -1) {
                    cycle(toCycle(index));
                    this.wire.bytes().readPosition(0);
                    return this;
                }
                if (!moveToIndex(index))
                    throw new IllegalStateException("unable to move to the start, cycle=" + cycle);
            }

            return this;
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() {
            // this is the last written index so the end is 1 + last written index
            final long index = queue.lastIndex();
            if (index == -1)
                return this;
            moveToIndex(index + 1);
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

        public RollingChronicleQueue queue() {
            return queue;
        }

        private <T> boolean read(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c) {
            long index = this.index;
            if (this.store == null) {
                index = queue.firstIndex();
                if (index == -1)
                    return false;
                moveToIndex(index);
            }

            if (read0(t, c)) {
                this.index = RollingChronicleQueue.index(this.cycle, toSequenceNumber(index) + 1);
                return true;
            }
            return false;
        }

        private <T> boolean read0(@NotNull final T t, @NotNull final BiConsumer<T, Wire> c) {
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
                cycle(roll);
                if (store == null)
                    return false;
            }
        }

        @NotNull
        private StoreTailer cycle(final long cycle) {
            if (cycle != 16843)
                throw new AssertionError();
            if (this.cycle != cycle) {
                if (this.store != null) {
                    this.queue.release(this.store);
                }
                this.cycle = cycle;
                this.store = this.queue.storeForCycle(cycle, queue.epoch());
                this.wire = queue.wireType().apply(store.mappedBytes());
                moveToIndex(RollingChronicleQueue.index(cycle, -1));
                if (LOG.isDebugEnabled())
                    LOG.debug("tailer=" + ((MappedBytes) wire.bytes()).mappedFile().file().getAbsolutePath());
            }
            return this;
        }
    }
}

