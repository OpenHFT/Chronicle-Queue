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

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * The main data container of a {@link ChronicleQueue}, an extended version of {@link ExcerptTailer}
 * which also facilitates random access.
 *
 * @author peter.lawrey
 */
public interface Excerpt extends ExcerptTailer {
    /**
     * Find any entry which return a match i.e. 0, or a negative value which is the boundary between
     * -1 and +1
     *
     * @param comparator to use for comparison.
     * @return 0 to size()-1 for a match, -1 to -size()-1 for index of closest match
     */
    long findMatch(@NotNull ExcerptComparator comparator);

    /**
     * Find entries which return a match.  This is a combined search which is usually faster than
     * two binary search. This doesn't access the same index two. The best case is one additional
     * comparison and the worst case is the same.
     *
     * @param startEnd   lower (inclusive) to upper (exclusive). Will be equal if no exact match is
     *                   found.
     * @param comparator to use for comparison.
     */
    void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator);

    /**
     * Randomly select an Excerpt.
     *
     * @param index index to look up
     * @return true if this is a valid entries and not padding.
     */
    boolean moveToIndex(long index) throws IOException;


    /**
     * Replay from the lower.
     *
     * @return this Excerpt
     */
    @NotNull
    Excerpt toStart();

    /**
     * Wind to the upper.
     *
     * @return this Excerpt
     */
    @NotNull
    Excerpt toEnd();
}
