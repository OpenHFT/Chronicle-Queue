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

import java.io.IOException;

public class SingleChronicleQueueExcerpts {

    /**
     * Appender
     */
    static class Appender implements ExcerptAppender {
        private final SingleChronicleQueue queue;

        private int cycle;
        private long index;
        private SingleChronicleQueueFormat format;

        Appender(SingleChronicleQueue queue) {
            this.queue = queue;

            this.cycle = 0;
            this.index = 0;
            this.format = null;
        }

        @Override
        public long writeDocument(WriteMarshallable writer) throws IOException {
            if(this.cycle != queue.cycle()) {
                this.cycle = queue.cycle();
                this.format = queue.formatForCycle(this.cycle);
            }

            index = format.append(writer);

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
        private SingleChronicleQueueFormat format;

        Tailer(SingleChronicleQueue queue) {
            this.queue = queue;

            this.cycle = 0;
            this.position = 0;
            this.format = null;
        }

        @Override
        public boolean readDocument(ReadMarshallable reader) throws IOException {
            if(this.cycle != queue.cycle()) {
                this.cycle = queue.cycle();
                this.format = queue.formatForCycle(this.cycle);
                this.position = format.dataPosition();
            }

            long result = format.read(this.position, reader);
            if(WireUtil.NO_DATA != result) {
                this.position = result;
                return true;
            }

            return false;
        }

        @Override
        public boolean index(long l) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExcerptTailer toStart() {
            this.position = format.dataPosition();
            return this;
        }

        @Override
        public ExcerptTailer toEnd() {
            this.position = format.writePosition();
            return this;
        }

        @Override
        public ChronicleQueue queue() {
            return this.queue;
        }
    }
}
