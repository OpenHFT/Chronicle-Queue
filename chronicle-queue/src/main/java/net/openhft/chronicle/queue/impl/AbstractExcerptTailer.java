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
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;


public abstract class AbstractExcerptTailer implements ExcerptTailer {
    @Nullable
    @Override
    public WireIn wire() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean readDocument(Consumer<WireIn> reader) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean index(long l) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public ExcerptTailer toStart() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @NotNull
    @Override
    public ExcerptTailer toEnd() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ChronicleQueue chronicle() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
