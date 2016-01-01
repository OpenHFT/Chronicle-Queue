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
import net.openhft.chronicle.core.annotation.ForceInline;
import net.openhft.chronicle.core.util.ThrowingAcceptor;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;

public class Excerpts {

    // *************************************************************************
    //
    // APPENDERS
    //
    // *************************************************************************

    public static class DefaultAppender<T extends ChronicleQueue> implements ExcerptAppender {
        protected final T queue;

        public DefaultAppender(@NotNull T queue) {
            this.queue = queue;
        }

        @Override
        public long writeDocument(@NotNull WriteMarshallable writer) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long writeBytes(@NotNull WriteBytesMarshallable marshallable) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long writeBytes(@NotNull Bytes<?> bytes) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long index() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cycle() {
            throw new UnsupportedOperationException();
        }

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

        public DelegatedAppender(
                @NotNull ChronicleQueue queue,
                @NotNull ExcerptAppender appender) throws IOException {

            super(queue);

            this.buffer = elasticByteBuffer();
            this.wire = queue.wireType().apply(this.buffer);
            this.writer = appender::writeBytes;
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
    }

    /**
     * StoreAppender
     */
    public static class StoreAppender extends DefaultAppender<AbstractChronicleQueue> {

        private final MappedBytes writeContext;
        private long cycle;
        private long index = -1;
        private WireStore store;

        public StoreAppender(@NotNull AbstractChronicleQueue queue) throws IOException {

            super(queue);

            this.cycle = super.queue.lastCycle();

            if (this.cycle == -1)
                this.cycle = queue.cycle();

            if (this.cycle <= 0)
                throw new IllegalArgumentException();

            this.store = queue.storeForCycle(this.cycle);
            this.index = this.store.lastIndex();

            final MappedFile mappedFile = store.mappedFile();
            System.out.println("appender file=" + mappedFile.file().getAbsolutePath());
            this.writeContext = new MappedBytes(mappedFile);
        }

        @Override
        public long writeDocument(@NotNull WriteMarshallable writer) throws IOException {
            return index = store().append(this.writeContext, writer);
        }

        @Override
        public long writeBytes(@NotNull WriteBytesMarshallable marshallable) throws IOException {
            return index = store().append(this.writeContext, marshallable);
        }

        @Override
        public long writeBytes(@NotNull Bytes bytes) throws IOException {
            return index = store().append(this.writeContext, bytes);
        }

        @Override
        public long index() {
            if (this.index == -1) {
                throw new IllegalStateException();
            }

            return this.index;
        }

        public static void main(String[] args) {
            System.out.println("" + (101 / 10));
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
                    this.store.appendRollMeta(this.writeContext, nextCycle);
                    this.queue.release(this.store);
                }

                this.cycle = nextCycle;
                this.store = queue.storeForCycle(this.cycle);
                //this.store.acquireBytesAtWritePositionForWrite(this.writeContext.bytes);
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
        private MappedBytes readContext;

        private long cycle;
        private long index;
        private WireStore store;

        //TODO: refactor
        private boolean toStart;

        public StoreTailer(@NotNull AbstractChronicleQueue queue) throws IOException {
            this.queue = queue;
            this.cycle = -1;

            toStart();

            //  this.store = this.cycle > 0 ? queue.storeForCycle(this.cycle) : null;
            //  this.index = this.cycle > 0 ? this.store.lastIndex() : -1;

            //  ;

        }

        @Override
        public boolean readDocument(@NotNull ReadMarshallable reader) throws IOException {
            if (this.store == null) {
                long lastCycle = this.toStart ? queue.firstCycle() : queue.lastCycle();
                if (lastCycle == -1) {
                    return false;
                }

                //TODO: what should be done at the beginning ? toEnd/toStart
                cycle(lastCycle);
                //     context(store::acquireBytesAtReadPositionForRead);
            }

            long position = store.read(readContext, reader);
            if (position > 0) {
                this.index++;

                return true;
            } else if (position < 0) {
                // roll detected, move to next cycle;
                cycle(Math.abs(position));
                //context(store::acquireBytesAtReadPositionForRead);

                // try to read from new cycle
                return readDocument(reader);
            }

            return false;
        }

        @Override
        public boolean readBytes(@NotNull ReadBytesMarshallable marshallable) throws IOException {
            if (this.store == null) {
                long lastCycle = this.toStart ? queue.firstCycle() : queue.lastCycle();
                if (lastCycle == -1) {
                    return false;
                }

                //TODO: what should be done at the beginning ? toEnd/toStart
                cycle(lastCycle);
                //      context(store::acquireBytesAtReadPositionForRead);
            }

            long position = store.read(readContext, marshallable);
            if (position > 0) {
                this.index++;

                return true;
            } else if (position < 0) {
                // roll detected, move to next cycle;
                cycle(Math.abs(position));
                //  context(store::acquireBytesAtReadPositionForRead);

                // try to read from new cycle
                return readBytes(marshallable);
            }

            return false;
        }

        @Override
        public long index() {
            //TODO: should we raise an exception ?
            //if(this.store == null) {
            //    throw new IllegalArgumentException("This tailer is not bound to any cycle");
            //}

            return this.index;
        }

        @Override
        public long cycle() {
            if (this.store == null) {
                throw new IllegalArgumentException("This tailer is not bound to any cycle");
            }

            return this.store.cycle();
        }

        @Override
        public boolean index(long index) throws IOException {
            if (this.store == null) {
                cycle(queue.lastCycle());
                //   context(store::acquireBytesAtReadPositionForRead);
            }

            //this.context.clear();

            if (this.store.moveToIndex(readContext, index)) {
                this.index = index - 1;
                return true;
            }

            return false;
        }

        @Override
        public boolean index(long cycle, long index) throws IOException {
            cycle(cycle);
            //   context(store::acquireBytesAtReadPositionForRead);

            return index(index);
        }

        @Override
        public ExcerptTailer toStart() throws IOException {
            long firstCycle = queue.firstCycle();
            if (firstCycle > 0) {
                cycle(firstCycle);
                this.toStart = false;
            } else {
                cycle(cycle());
                this.toStart = true;
            }

            this.index = -1;

            return this;
        }

        @Override
        public ExcerptTailer toEnd() throws IOException {
            long cycle = queue.lastCycle();
            if (cycle > 0) {
                cycle(cycle);
            }

            this.toStart = false;

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
                this.index = -1;
                this.store = this.queue.storeForCycle(this.cycle);
                final MappedFile mappedFile = store.mappedFile();
                this.readContext = new MappedBytes(mappedFile);

                System.out.println("tailer=" + mappedFile.file().getAbsolutePath().toString());

            }

            return this;
        }

        private StoreTailer context(@NotNull ThrowingAcceptor<Bytes, IOException> acceptor)
                throws IOException {
            acceptor.accept(readContext);
            return this;
        }
    }
}
