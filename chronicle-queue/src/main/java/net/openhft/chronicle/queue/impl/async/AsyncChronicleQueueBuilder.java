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

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class AsyncChronicleQueueBuilder implements ChronicleQueueBuilder {
    private final ChronicleQueue queue;
    private long bufferSize;

    public AsyncChronicleQueueBuilder(@NotNull ChronicleQueue queue) {
        this.queue = queue;
        this.bufferSize = 640_000;
    }

    public long bufferSize() {
        return this.bufferSize;
    }

    public AsyncChronicleQueueBuilder bufferSize(long bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    @NotNull
    public ChronicleQueue build() throws IOException {
        return new AsyncChronicleQueue(queue, this.bufferSize);
    }

    @NotNull
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public AsyncChronicleQueueBuilder clone() {
        try {
            return (AsyncChronicleQueueBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }
}
