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

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.Excerpt;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public abstract class AbstractChronicleQueue implements ChronicleQueue {
    @NotNull
    @Override
    public String name() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public abstract long epoch();

    @NotNull
    @Override
    public ExcerptAppender createAppender() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long size() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public long firstIndex() {
        throw new UnsupportedOperationException("Not implemented");
    }

    public long lastIndex() {
        throw new UnsupportedOperationException("Not implemented");
    }


    @Override
    @NotNull
    public WireType wireType() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * @param cycle the cycle
     * @param epoch an epoch offset as the number of number of milliseconds since January 1, 1970,
     *              00:00:00 GMT
     * @return the {@code WireStore} associated with this {@code cycle}
     */
    @NotNull
    protected abstract WireStore storeForCycle(long cycle, final long epoch);

    /**
     * @param store the {@code store} to release
     */
    protected abstract void release(@NotNull WireStore store);

    /**
     * @return the current cycle
     */
    protected abstract long cycle();


}
