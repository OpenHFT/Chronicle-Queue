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

/**
 * For a binary search, provide a comparison of Excerpts
 */
public interface ExcerptComparator {
    /**
     * Given some criteria, determine if the entry is -1 = below range, +1 = above range and 0 in
     * range Can be used for exact matches or a range of values.
     *
     * @param excerpt to check
     * @return -1 below, 0 = in range, +1 above range.
     */
    int compare(@NotNull Excerpt excerpt);
}
