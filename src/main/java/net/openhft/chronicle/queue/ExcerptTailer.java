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
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ReadMarshallable;
import org.jetbrains.annotations.NotNull;

/**
 * The component that facilitates sequentially reading data from a {@link ChronicleQueue}.
 *
 * @author peter.lawrey
 */
public interface ExcerptTailer extends ExcerptCommon {
    /**
     * @param reader user to read the document
     * @return {@code true} if successful
     */
    boolean readDocument(@NotNull ReadMarshallable reader);

    /**
     * @param marshallable used to read the document
     * @return {@code true} if successful
     */
    boolean readBytes(@NotNull ReadBytesMarshallable marshallable);

    /**
     * @param using used to read the document
     * @return {@code true} if successful
     */
    boolean readBytes(@NotNull Bytes using);

    /**
     * equivalent to {@link  ExcerptTailer#readDocument(ReadMarshallable)} but with out the use of a
     * lambda expression.
     *
     * This method is the ExcerptTailer equivalent of {@link net.openhft.chronicle.wire.WireIn#readingDocument()}
     *
     * @return the document context
     */
    DocumentContext readingDocument();

    /**
     * @return the index just read , this include the cycle and the sequence number from with this
     * cycle
     */
    long index();

    /**
     * Randomly select an Excerpt.
     *
     * @param index index to look up, the index includes the cycle number and a sequence number from
     *              with this cycle
     * @return true if this is a valid entries.
     */
    boolean moveToIndex(long index);

    /**
     * Replay from the lower.
     *
     * @return this Excerpt
     */
    @NotNull
    ExcerptTailer toStart();

    /**
     * Wind to the upper.
     *
     * @return this Excerpt
     */
    @NotNull
    ExcerptTailer toEnd();
}
