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
import net.openhft.chronicle.queue.impl.AbstractChronicleQueue;
import net.openhft.chronicle.queue.impl.AbstractExcerptAppender;
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

    // *************************************************************************
    //
    // *************************************************************************

    private class Appender extends AbstractExcerptAppender {
        @Override
        public void writeDocument(WriteMarshallable writer) {
            try {
                format().append(writer);
            } catch(IOException e) {
                //TODO: should this method throw an exception ?
            }
        }

        @Override
        public ChronicleQueue chronicle() {
            return SingleChronicleQueue.this;
        }
    }
}
