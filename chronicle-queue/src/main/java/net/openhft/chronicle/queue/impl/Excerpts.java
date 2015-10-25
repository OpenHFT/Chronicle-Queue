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
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

public class Excerpts {

    // *************************************************************************
    //
    // APPENDERS
    //
    // *************************************************************************

    static class DefaultAppender<T extends ChronicleQueue> implements ExcerptAppender {
        protected final T queue;

        public DefaultAppender(@NotNull T queue) {
            this.queue = queue;
        }

        @Override
        public long writeDocument(WriteMarshallable writer) throws IOException {
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

    public static class DelegatedAppender extends DefaultAppender<ChronicleQueue> {
        private final Bytes buffer;
        private final Wire wire;
        private final Consumer<Bytes> consumer;

        public DelegatedAppender(
                @NotNull ChronicleQueue queue,
                @NotNull Consumer<Bytes> consumer) throws IOException {

            super(queue);

            this.buffer = NativeBytes.nativeBytes();
            this.wire = queue.wireType().apply(this.buffer);
            this.consumer = consumer;
        }

        @Override
        public long writeDocument(WriteMarshallable writer) throws IOException {
            buffer.clear();
            writer.writeMarshallable(wire);
            consumer.accept(buffer);

            return WireConstants.NO_INDEX;
        }
    }

    /**
     * StoreAppender
     */
    public static class StoreAppender extends DefaultAppender<AbstractChronicleQueue> {
        private long cycle;
        private long index;
        private WireStore store;

        StoreAppender(
                @NotNull AbstractChronicleQueue queue) throws IOException {

            super(queue);

            this.cycle = super.queue.lastCycle();
            this.store = this.cycle > 0 ? queue.storeForCycle(this.cycle) : null;
            this.index = this.cycle > 0 ? this.store.lastIndex() : -1;
        }

        @Override
        public long writeDocument(@NotNull WriteMarshallable writer) throws IOException {
            if(this.cycle != queue.cycle()) {
                long nextCycle = queue.cycle();
                if(this.store != null) {
                    this.store.appendRollMeta(nextCycle);
                    this.queue.release(this.store);
                }

                this.cycle = nextCycle;
                this.store = queue.storeForCycle(this.cycle);
            }

            index = store.append(writer);

            return index;
        }

        //TODO: refactor
        public boolean write(
                @NotNull ThrowingFunction<BytesProvider, IllegalStateException, Bytes> bytesFunction)
                throws IOException {

            if(this.cycle != queue.cycle()) {
                long nextCycle = queue.cycle();
                if(this.store != null) {
                    this.store.appendRollMeta(nextCycle);
                    this.queue.release(this.store);
                }

                this.cycle = nextCycle;
                this.store = queue.storeForCycle(this.cycle);
            }

            final Bytes bytes = bytesFunction.apply(store::acquire);
            if(!bytes.isClear()) {
                bytes.writeVolatileInt(
                    bytes.start(),
                    Wires.toIntU30(bytes.writePosition() - bytes.start() + 4, "TODO"));

                return true;
            }

            return false;
        }

        @Override
        public long index() {
            if (this.index == -1) {
                throw new IllegalStateException();
            }

            return this.index;
        }

        @Override
        public long cycle() {
            return this.store.cycle();
        }

        @Override
        public ChronicleQueue queue() {
            return this.queue;
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

        private long cycle;
        private long index;
        private long position;
        private WireStore store;

        //TODO: refactor
        private boolean toStart;

        StoreTailer(@NotNull AbstractChronicleQueue queue) throws IOException {
            this.queue = queue;
            this.cycle = -1;
            this.index = -1;
            this.store = null;
            this.position = 0;
            this.toStart = false;
        }

        @Override
        public boolean readDocument(ReadMarshallable reader) throws IOException {
            if(this.store == null) {
                long lastCycle = this.toStart ? queue.firstCycle() : queue.lastCycle();
                if(lastCycle == -1) {
                    return false;
                }

                //TODO: what should be done at the beginning ? toEnd/toStart
                cycle(lastCycle, WireStore::readPosition);
            }

            long position = store.read(this.position, reader);
            if(position > 0) {
                this.position = position;
                this.index++;

                return true;
            } else if(position < 0) {
                // roll detected, move to next cycle;
                cycle(Math.abs(position), WireStore::readPosition);

                // try to read from new cycle
                return readDocument(reader);
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
            if(this.store == null) {
                throw new IllegalArgumentException("This tailer is not bound to any cycle");
            }

            return this.store.cycle();
        }

        @Override
        public boolean index(long index) throws IOException {
            if(this.store == null) {
                cycle(queue.lastCycle(), WireStore::readPosition);
            }

            long idxpos = this.store.positionForIndex(index);
            if(idxpos != WireConstants.NO_INDEX) {
                this.position = idxpos;
                this.index = index - 1;

                return true;
            }

            return false;
        }

        @Override
        public boolean index(long cycle, long index) throws IOException {
            cycle(cycle, WireStore::readPosition);
            return index(index);
        }

        @Override
        public ExcerptTailer toStart() throws IOException {
            long firstCycle = queue.firstCycle();
            if(firstCycle > 0) {
                cycle(firstCycle, WireStore::readPosition);
                this.toStart = false;
            } else {
                this.toStart = true;
            }

            return this;
        }

        @Override
        public ExcerptTailer toEnd() throws IOException {
            long firstCycle = queue.firstCycle();
            if(firstCycle > 0) {
                cycle(firstCycle, WireStore::writePosition);
            }

            this.toStart = false;

            return this;
        }

        @Override
        public ChronicleQueue queue() {
            return this.queue;
        }

        private void cycle(long cycle, @NotNull ToLongFunction<WireStore> positionSupplier) throws IOException {
            if(this.cycle != cycle) {
                if(null != this.store) {
                    this.queue.release(this.store);
                }

                this.cycle = cycle;
                this.index = -1;
                this.store = this.queue.storeForCycle(this.cycle);
                this.position = positionSupplier.applyAsLong(this.store);
            }
        }
    }
}
