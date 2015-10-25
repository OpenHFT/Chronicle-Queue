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

import net.openhft.chronicle.bytes.Bytes;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

@FunctionalInterface
public interface BytesProvider {

    /**
     * sets up a buffer to back the ring buffer, the data wil be read into this
     * buffer the size of the buffer must be as big as {@code maxSize}
     *
     * @param maxSize the number of bytes required
     * @return a buffer of at least {@code maxSize} bytes remaining
     */
    @NotNull
    Bytes provide(long maxSize) throws IOException;
}

