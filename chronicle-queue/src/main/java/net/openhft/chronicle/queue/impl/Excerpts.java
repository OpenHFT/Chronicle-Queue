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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
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
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;
import static net.openhft.chronicle.queue.ChronicleQueue.*;

public class Excerpts {


    private static final Logger LOG = LoggerFactory.getLogger(Excerpts.class);


    // *************************************************************************
    //
    // APPENDERS
    //
    // *************************************************************************

    public static abstract class DefaultAppender<T extends ChronicleQueue> implements ExcerptAppender {
        protected final T queue;

        public DefaultAppender(@NotNull T queue) {
            this.queue = queue;
        }

        @Override
        public long writeDocument(@NotNull WriteMarshallable writer) throws IOException {
            throw new UnsupportedOperationException();
        }

        //   @Override
        ///    public long writeBytes(@NotNull WriteBytesMarshallable marshallable) throws IOException {
        //       throw new UnsupportedOperationException();
        //   }

        @Override
        public long writeBytes(@NotNull Bytes<?> bytes) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long index() {
            throw new UnsupportedOperationException();
        }


        public abstract long cycle();

        @Override
        public ChronicleQueue queue() {
            return this.queue;
        }
    }

    /**
     * Delegates the appender
     */
    public static class DelegatedAppender extends DefaultAppender<ChronicleQueue> {
        private final Bytes<ByteBuffer> buffer;
        private final Wire wire;
        private final BytesWriter writer;

        public DelegatedAppender(
                @NotNull ChronicleQueue queue,
                @NotNull BytesWriter writer) throws IOException {

            super(queue);

            this.buffer = elasticByteBuffer();
            this.wire = queue.wireType().apply(this.buffer);
            this.writer = writer;
        }


        @Override
        public long writeDocument(@NotNull WriteMarshallable writer) throws IOException {
            this.buffer.clear();
            writer.writeMarshallable(this.wire);
            this.buffer.readLimit(this.buffer.writePosition());
            this.buffer.readPosition(0);
            this.buffer.writePosition(this.buffer.readLimit());
            this.buffer.writeLimit(this.buffer.readLimit());

            return writeBytes(this.buffer);
        }


        @Override
        public long writeBytes(@NotNull WriteBytesMarshallable marshallable) throws IOException {
            this.buffer.clear();
            marshallable.writeMarshallable(this.buffer);
            this.buffer.readLimit(this.buffer.writePosition());
            this.buffer.readPosition(0);
            this.buffer.writePosition(this.buffer.readLimit());
            this.buffer.writeLimit(this.buffer.readLimit());
            return writeBytes(this.buffer);
        }

        @Override
        public long writeBytes(@NotNull Bytes<?> bytes) throws IOException {
            return writer.write(bytes);
        }

        @Override
        public long cycle() {
            throw new UnsupportedOperationException("todo");
        }
    }

    /**
     * StoreAppender
     */
    public static class StoreAppender extends DefaultAppender<AbstractChronicleQueue> {


        private Wire wire;
        private long epoch;
        private long cycle;
        private long index = -1;
        private WireStore store;

        public StoreAppender(@NotNull AbstractChronicleQueue queue) throws IOException {

            super(queue);

            final long lastIndex = super.queue.lastIndex();
            this.cycle = (lastIndex == -1) ? queue.cycle() : toCycle(lastIndex);

            if (this.cycle < 0)
                throw new IllegalArgumentException("You can not have a cycle that starts " +
                        "before Epoch. cycle=" + cycle);

            this.store = queue.storeForCycle(this.cycle, this.epoch);
            this.index = this.store.lastSubIndex();

            final MappedBytes mappedBytes = store.mappedBytes();
            if (LOG.isDebugEnabled())
                LOG.debug("appender file=" + mappedBytes.mappedFile().file().getAbsolutePath());

            wire = this.queue().wireType().apply(mappedBytes);
        }

        public long writeDocument(@NotNull WriteMarshallable writer) throws IOException {
            final WireStore store = store();
            long position = wire.bytes().writePosition();
            wire.writeDocument(false, writer);
            final long index = store.incrementLastIndex();
            this.index = index;
            store.storeIndexLocation(wire, position, index);
            return ChronicleQueue.index(store.cycle(), index);
        }

        @Override
        public long writeBytes(@NotNull WriteBytesMarshallable marshallable) throws IOException {
            return writeDocument(wire1 -> marshallable.writeMarshallable(wire1.bytes()));
        }

        @Override
        public long writeBytes(@NotNull Bytes bytes) throws IOException {
            return writeDocument(wire1 -> wire1.bytes().write(bytes));
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

        @Override
        public ChronicleQueue queue() {
            return this.queue;
        }

        @ForceInline
        private WireStore store() throws IOException {
            if (this.cycle != queue.cycle()) {
                long nextCycle = queue.cycle();
                if (this.store != null) {
                    this.store.appendRollMeta(wire, nextCycle);
                    this.queue.release(this.store);
                }

                this.cycle = nextCycle;
                this.store = queue.storeForCycle(this.cycle, epoch);
                this.wire = this.queue().wireType().apply(store.mappedBytes());
            }

            return this.store;
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
        private final AbstractChronicleQueue queue;
        private Wire wire;

        private long cycle;

        private long index;
        private WireStore store;

        public StoreTailer(@NotNull AbstractChronicleQueue queue) throws IOException {
            this.queue = queue;
            this.cycle = -1;

            toStart();
        }

        @Override
        public boolean readDocument(@NotNull ReadMarshallable marshaller) throws IOException {
            return readAtIndex(marshaller::readMarshallable);
        }

        @Override
        public boolean readBytes(@NotNull Bytes using) throws IOException {
            return readAtIndex(w -> using.write(w.bytes()));
        }

        @Override
        public boolean readBytes(@NotNull ReadBytesMarshallable using) throws IOException {
            return readAtIndex(w -> using.readMarshallable(w.bytes()));
        }

        private boolean readAtIndex(@NotNull Consumer<Wire> c) throws IOException {

            final long readPosition = wire.bytes().readPosition();
            final long readLimit = wire.bytes().readLimit();
            final long cycle = this.cycle;
            final long index = this.index;

            if (this.store == null) {
                final long firstIndex = queue.firstIndex();
                if (index == -1) {
                    return false;
                }
                moveToIndex(firstIndex);
            }

            final boolean success = readAtSubIndex(c);

            if (success) {

                this.index = index(cycle, toSubIndex(index) + 1);
                return true;
            }
            // roll detected, move to next cycle;

            //context(store::acquireBytesAtReadPositionForRead);
            cycle(cycle);
            wire.bytes().readLimit(readLimit);
            wire.bytes().readPosition(readPosition);

            return false;
        }

        private boolean readAtSubIndex(@NotNull Consumer<Wire> c) throws IOException {

            long roll;
            for (; ; ) {
                roll = Long.MIN_VALUE;
                wire.bytes().readLimit(wire.bytes().capacity());

                while (wire.bytes().readLong(wire.bytes().readPosition()) != 0) {

                    try (@NotNull final DocumentContext documentContext = wire.readingDocument()) {

                        if (!documentContext.isPresent())
                            throw new IllegalStateException("document not present");

                        if (documentContext.isData()) {
                            c.accept(wire);
                            return true;
                        }

                        // In case of meta data, if we are found the "roll" meta, we returns
                        // the next cycle (negative)
                        final StringBuilder sb = Wires.acquireStringBuilder();

                        final ValueIn vi = wire.readEventName(sb);
                        if ("roll".contentEquals(sb)) {
                            roll = vi.int32();
                            break;
                        }

                    }

                }

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
        public long moveToIndex() {
            //TODO: should we raise an exception ?
            if (this.store == null) {
                throw new IllegalArgumentException("This tailer is not bound to any cycle");
            }

            return index(this.cycle, this.index);
        }


        @Override
        public boolean moveToIndex(long index) throws IOException {

            if (LOG.isDebugEnabled()) {
                LOG.debug(SingleChronicleQueueStore.IndexOffset.toBinaryString(index));
                LOG.debug(SingleChronicleQueueStore.IndexOffset.toScale());
            }


            final long expectedCycle = toCycle(index);
            if (expectedCycle != cycle)
                // moves to the expected cycle
                cycle(expectedCycle);

            cycle = expectedCycle;


            final Bytes<?> bytes = wire.bytes();

            final long subIndex = toSubIndex(index);
            if (subIndex == -1) {
                bytes.readPosition(0);
                this.index = index(cycle, subIndex);
                return true;
            }

            final long position = this.store.moveToIndex(wire, ChronicleQueue.toSubIndex(index));

            if (position == -1)
                return false;

            bytes.readPosition(position);
            bytes.readLimit(bytes.capacity());
            this.index = index(cycle, subIndex - 1);
            return true;
        }


        @NotNull
        @Override
        public ExcerptTailer toStart() throws IOException {

            final long index = queue.firstIndex();
            if (ChronicleQueue.toSubIndex(index) == -1) {
                cycle(toCycle(index));
                this.wire.bytes().readPosition(0);
                return this;
            }

            LOG.info("index=> index=" + toSubIndex(index) + ",cycle=" +
                    toCycle(index));


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

        @Override
        public ChronicleQueue queue() {
            return this.queue;
        }

        private StoreTailer cycle(long cycle) throws IOException {
            if (this.cycle != cycle) {
                if (null != this.store) {
                    this.queue.release(this.store);
                }
                this.cycle = cycle;
                //  this.index = -1;
                this.store = this.queue.storeForCycle(cycle, queue.epoch());

                wire = queue.wireType().apply(store.mappedBytes());
                moveToIndex(index(cycle, -1));
                if (LOG.isDebugEnabled())
                    LOG.debug("tailer=" + ((MappedBytes) wire.bytes()).mappedFile().file().getAbsolutePath());

            }

            return this;
        }


    }
}

