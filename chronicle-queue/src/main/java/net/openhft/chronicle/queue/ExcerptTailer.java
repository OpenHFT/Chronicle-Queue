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
     *
     * @param reader
     * @return
     * @throws IOException
     */
    boolean readDocument(@NotNull ReadMarshallable reader) throws IOException;

    /**
     * Randomly select an Excerpt.
     *
     * @param l index to look up
     * @return true if this is a valid entries and not padding.
     */
    boolean index(long l) throws IOException;;

    /**
     * Replay from the lower.
     *
     * @return this Excerpt
     */
    @NotNull
    ExcerptTailer toStart() throws IOException;;

    /**
     * Wind to the upper.
     *
     * @return this Excerpt
     */
    @NotNull
    ExcerptTailer toEnd() throws IOException;;
}
