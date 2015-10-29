/*
 *
 *    Copyright (C) 2015  higherfrequencytrading.com
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Lesser General Public License as published by
 *    the Free Software Foundation, either version 3 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package net.openhft.chronicle.queue.impl.async;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytesStore;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.DelegatedChronicleQueue;
import net.openhft.chronicle.queue.impl.Excerpts;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;
import net.openhft.chronicle.threads.EventGroup;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AsyncChronicleQueue extends DelegatedChronicleQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncChronicleQueue.class);

    private final NativeBytesStore store;
    private final BytesRingBuffer buffer;
    private final ExcerptAppender storeAppender;
    private final EventGroup eventGroup;
    private ExcerptAppender appender;

    public AsyncChronicleQueue(@NotNull ChronicleQueue queue, long capacity) throws IOException {
        super(queue);

        this.store = NativeBytesStore.nativeStoreWithFixedCapacity(capacity);
        this.store.zeroOut(0, this.store.writeLimit());
        this.buffer = new BytesRingBuffer(this.store);
        this.appender = null;
        this.storeAppender = super.createAppender();
        this.eventGroup = new EventGroup(true);
        this.eventGroup.addHandler(this::handleEvent);
        this.eventGroup.start();
    }

    @NotNull
    @Override
    public synchronized  ExcerptAppender createAppender() throws IOException {
        if(appender != null) {
            //TODO: better error management
            throw new IllegalStateException("Max 1 appender per queue");
        }

        return this.appender = new Excerpts.DelegatedAppender(this, bytes -> {
            try {
                this.buffer.offer(bytes);
            } catch(InterruptedException e) {
                //TODO: what to do ?
                LOGGER.warn("", e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        this.eventGroup.close();
        super.close();
    }

    // *************************************************************************
    //
    // *************************************************************************

    private boolean handleEvent() throws InvalidEventHandlerException {
        try {
            return buffer.apply(this::appendBytes) > 0;
        } catch(InterruptedException e) {
            //TODO: what to do
            LOGGER.warn("", e);
        }

        return false;
    }

    private void appendBytes(Bytes<?> bytes) {
        try {
            storeAppender.writeBytes(bytes);
        } catch(IOException e) {
            //TODO: what to do
            LOGGER.warn("", e);
        }
    }
}
