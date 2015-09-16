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
import net.openhft.chronicle.queue.impl.AbstractChronicleQueue;
import net.openhft.chronicle.queue.impl.AbstractExcerptAppender;
import net.openhft.chronicle.queue.impl.AbstractExcerptTailer;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WireUtil;
import net.openhft.chronicle.wire.WriteMarshallable;

import java.io.IOException;

class SingleChronicleQueue extends AbstractChronicleQueue<SingleChronicleQueueFormat> {

    protected SingleChronicleQueue(final SingleChronicleQueueBuilder builder) throws IOException {
        super(SingleChronicleQueueFormat.from(builder));
    }

    @Override
    public ExcerptAppender createAppender() {
        return new Appender();
    }

    @Override
    public ExcerptTailer createTailer() {
        return new Tailer();
    }

    // *************************************************************************
    //
    // *************************************************************************

    private class Appender extends AbstractExcerptAppender {
        Appender() {
            super( SingleChronicleQueue.this);
        }

        @Override
        public void writeDocument(WriteMarshallable writer) {
            try {
                format().append(writer);
            } catch(IOException e) {
                //TODO: should this method throw an exception ?
            }
        }

        @Override
        public ChronicleQueue queue() {
            return SingleChronicleQueue.this;
        }
    }

    private class Tailer extends AbstractExcerptTailer {
        private long position;

        Tailer() {
            super(SingleChronicleQueue.this);

            this.position = format().dataPosition();
        }

        @Override
        public boolean readDocument(ReadMarshallable reader) {
            long result = format().read(this.position, reader);
            if(WireUtil.NO_DATA != result) {
                this.position = result;
                return true;
            }

            return false;
        }
    }
}
