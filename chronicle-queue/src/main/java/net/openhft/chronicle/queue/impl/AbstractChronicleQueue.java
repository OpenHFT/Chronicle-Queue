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
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public abstract class AbstractChronicleQueue<T extends ChronicleQueueFormat> implements ChronicleQueue {

    private final T format;

    protected AbstractChronicleQueue(final T format) {
        this.format = format;
    }

    @Override
    public String name() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
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
    public long firstAvailableIndex() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long lastWrittenIndex() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected T format() {
        return this.format;
    }
}
