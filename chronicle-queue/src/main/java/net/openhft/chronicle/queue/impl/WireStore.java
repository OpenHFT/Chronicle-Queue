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

import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public interface WireStore extends ReferenceCounted  {

    /**
     *
     * @return
     */
    int cycle();

    /**
     *
     * @return
     */
    long readPosition();

    /**
     *
     * @return
     */
    long writePosition();

    /**
     *
     * @return
     */
    long lastIndex();


    /**
     *
     * @param cycle
     * @return
     * @throws IOException
     */
    boolean appendRollMeta(int cycle) throws IOException;

    /**
     *
     * @param writer
     * @return
     * @throws IOException
     */
    long append(@NotNull WriteMarshallable writer) throws IOException;

    /**
     *
     * @param position
     * @param reader
     * @return
     * @throws IOException
     */
    long read(long position, @NotNull ReadMarshallable reader) throws IOException;

    /**
     *
     * @param index
     * @return
     */
    long positionForIndex(long index);
}
