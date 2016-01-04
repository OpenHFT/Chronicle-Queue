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
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.Excerpt;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class DelegatedChronicleQueue implements ChronicleQueue {
    private final ChronicleQueue queue;

    public DelegatedChronicleQueue(@NotNull ChronicleQueue queue) {
        this.queue = queue;
    }

    @Override
    public String name() {
        return queue.name();
    }

    @Override
    @NotNull
    public Excerpt createExcerpt() throws IOException {
        return queue.createExcerpt();
    }

    @Override
    @NotNull
    public ExcerptTailer createTailer() throws IOException {
        return queue.createTailer();
    }

    @Override
    @NotNull
    public ExcerptAppender createAppender() throws IOException {
        return queue.createAppender();
    }

    @Override
    public long size() {
        return queue.size();
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public long firstAvailableIndex() {
        return queue.firstAvailableIndex();
    }

    @Override
    public long lastWrittenIndex() {
        return queue.lastWrittenIndex();
    }

    @Override
    public void close() throws IOException {
        queue.close();
    }

    @Override
    public WireType wireType() {
        return queue.wireType();
    }
}
