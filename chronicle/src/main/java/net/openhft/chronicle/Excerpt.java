/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import net.openhft.lang.model.constraints.NotNull;

/**
 * @author peter.lawrey
 */
public interface Excerpt extends ExcerptTailer {
    /**
     * Find any entry which return a match i.e. 0, or a negative value which is the boundary between -1 and +1
     *
     * @param comparator to use for comparison.
     * @return 0 to size()-1 for a match, -1 to -size()-1 for index of closest match
     */
    long findMatch(@NotNull ExcerptComparator comparator);

    /**
     * Find entries which return a match.  This is a combined search which is usually faster than two binary search.
     * This doesn't access the same index two. The best case is one additional comparison and the worst case is the same.
     *
     * @param startEnd   start (inclusive) to end (enclusive). Will be equal if no exact match is found.
     * @param comparator to use for comparison.
     */
    void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator);

    /**
     * Randomly select an Excerpt.
     *
     * @param l index to look up
     * @return true if this is a valid entries and not padding.
     */
    boolean index(long l);

    /**
     * Replay from the start.
     *
     * @return this Excerpt
     */
    @NotNull
    Excerpt toStart();

    /**
     * Wind to the end.
     *
     * @return this Excerpt
     */
    @NotNull
    Excerpt toEnd();
}
