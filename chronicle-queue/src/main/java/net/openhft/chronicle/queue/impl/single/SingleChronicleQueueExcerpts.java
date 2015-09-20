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
package net.openhft.chronicle.queue.impl.single;


import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WireUtil;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleChronicleQueueExcerpts {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleChronicleQueueExcerpts.class);

    /**
     * Appender
     */
    static class Appender implements ExcerptAppender {
        private final SingleChronicleQueue queue;

        private int cycle;
        private long index;
        private SingleChronicleQueueStore store;

        Appender(SingleChronicleQueue queue) throws IOException {
            this.queue = queue;

            this.cycle = queue.lastCycle();
            this.store = this.cycle != 0 ? queue.storeForCycle(this.cycle) : null;
            this.index = this.cycle != 0 ? this.store.lastIndex() : -1;
        }

        @Override
        public long writeDocument(WriteMarshallable writer) throws IOException {
            if(this.cycle != queue.cycle()) {

                int nextCycle = queue.cycle();
                if(this.store != null) {
                    this.store.appendRollMeta(nextCycle);
                }

                this.cycle = nextCycle;
                this.store = queue.storeForCycle(this.cycle);
            }

            index = store.append(writer);

            return index;
        }

        @Override
        public long lastWrittenIndex() {
            return this.index;
        }

        @Override
        public ChronicleQueue queue() {
            return this.queue;
        }
    }


    /**
     * Tailer
     */
    static class Tailer implements ExcerptTailer {
        private final SingleChronicleQueue queue;

        private int cycle;
        private long position;
        private SingleChronicleQueueStore store;

        Tailer(SingleChronicleQueue queue) throws IOException {
            this.queue = queue;
            this.cycle = 0;
            this.store = null;
            this.position = 0;
        }

        @Override
        public boolean readDocument(ReadMarshallable reader) throws IOException {
            if(this.store == null) {
                //TODO: what should be done at the beginning ? toEnd/toStart
                cycle(queue.lastCycle());
                this.position = this.store.dataPosition();
            }

            long position = store.read(this.position, reader);
            if(position > 0) {
                this.position = position;
                return true;
            } else if(position < 0) {
                // roll detected, move to next cycle;
                cycle((int) Math.abs(position));
                this.position = this.store.dataPosition();

                // try to read from new cycle
                return readDocument(reader);
            }

            return false;
        }

        @Override
        public boolean index(long index) throws IOException {
            if(this.store == null) {
                cycle(queue.lastCycle());
                this.position = this.store.dataPosition();
            }

            long idxpos = this.store.positionForIndex(index);
            if(idxpos != WireUtil.NO_INDEX) {
                this.position = idxpos;

                return true;
            }

            return false;
        }

        @Override
        public ExcerptTailer toStart() throws IOException {
            cycle(queue.firstCycle());
            this.position = store.dataPosition();

            return this;
        }

        @Override
        public ExcerptTailer toEnd() throws IOException {
            cycle(queue.lastCycle());
            this.position = store.writePosition();

            return this;
        }

        @Override
        public ChronicleQueue queue() {
            return this.queue;
        }

        private void cycle(int cycle) throws IOException {
            this.cycle = cycle;
            this.store = queue.storeForCycle(this.cycle);
        }
    }
}
