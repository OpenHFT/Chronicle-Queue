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
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.wire.ReadMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * The component that facilitates sequentially reading data from a {@link ChronicleQueue}.
 *
 * @author peter.lawrey
 */
public interface ExcerptTailer extends ExcerptCommon {
    /**
     * @param reader user to read the document
     * @return {@code true} if successful
     * @throws IOException if not able to read the chronicle file
     */
    boolean readDocument(@NotNull ReadMarshallable reader) throws IOException;

    /**
     * @param marshallable used to read the document
     * @return {@code true} if successful
     * @throws IOException if not able to read the chronicle file
     */
    boolean readBytes(@NotNull ReadBytesMarshallable marshallable) throws IOException;

    /**
     * @param using used to read the document
     * @return {@code true} if successful
     * @throws IOException if not able to read the chronicle file
     */
    boolean readBytes(@NotNull Bytes using) throws IOException;


    /**
     * @return the index just read
     */
    long index();


    /**
     * Randomly select an Excerpt.
     *
     * @param index index to look up, the index includes the cycle number and a sequence number from
     *              with this cycle
     * @return true if this is a valid entries.
     * @throws IOException if not able to read the chronicle file
     */
    boolean moveToIndex(long index) throws IOException;


    /**
     * Replay from the lower.
     *
     * @return this Excerpt
     * @throws IOException if not able to read the chronicle file
     */
    @NotNull
    ExcerptTailer toStart() throws IOException;

    /**
     * Wind to the upper.
     *
     * @return this Excerpt
     * @throws IOException if not able to read the chronicle file
     */
    @NotNull
    ExcerptTailer toEnd() throws IOException;
}
