/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

/**
 * For a binary search, provide a comparison of Excerpts
 */
public interface ExcerptComparator {
    /**
     * Given some criteria, deterime if the entry is -1 = below range, +1 = above range and 0 in range
     * Can be used for exact matches or a range of values.
     *
     * @param excerpt to check
     * @return -1 below, 0 = in range, +1 above range.
     */
    int compare(Excerpt excerpt);
}
