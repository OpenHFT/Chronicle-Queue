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
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public abstract class AbstractExcerptAppender implements ExcerptAppender {
    private final ChronicleQueue queue;

    public AbstractExcerptAppender(@NotNull ChronicleQueue queue) {
        this.queue = queue;
    }

    @Nullable
    @Override
    public WireOut wire() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long writeDocument(WriteMarshallable writer) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long lastWrittenIndex() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ChronicleQueue queue() {
        return this.queue;
    }
}
