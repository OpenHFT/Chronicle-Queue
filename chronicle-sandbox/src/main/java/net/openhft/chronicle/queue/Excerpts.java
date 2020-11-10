/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import org.jetbrains.annotations.NotNull;

public enum Excerpts {
    ; // Utility class

    public static long findMatch(@NotNull Excerpt excerpt, @NotNull ExcerptComparator comparator) {
        long lo = excerpt.chronicle().firstAvailableIndex();
        long hi = excerpt.chronicle().lastWrittenIndex();
        while (lo <= hi) {
            long mid = (hi + lo) >>> 1;
            if (!excerpt.index(mid)) {
                if (mid > lo)
                    excerpt.index(--mid);
                else
                    break;
            }
            int cmp = comparator.compare(excerpt);
            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else
                return mid; // key found
        }
        return ~lo; // -(lo + 1)
    }

    public static void findRange(@NotNull Excerpt excerpt, @NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
        // lower search range
        long lo1 = excerpt.chronicle().firstAvailableIndex();
        long hi1 = excerpt.chronicle().lastWrittenIndex();
        // upper search range
        long lo2 = 0, hi2 = hi1;
        boolean both = true;
        // search for the low values.
        while (lo1 <= hi1) {
            long mid = (hi1 + lo1) >>> 1;
            if (!excerpt.index(mid)) {
                if (mid > lo1)
                    excerpt.index(--mid);
                else
                    break;
            }
            int cmp = comparator.compare(excerpt);

            if (cmp < 0) {
                lo1 = mid + 1;
                if (both)
                    lo2 = lo1;

            } else if (cmp > 0) {
                hi1 = mid - 1;
                if (both)
                    hi2 = hi1;

            } else {
                hi1 = mid - 1;
                if (both)
                    lo2 = mid + 1;
                both = false;
            }
        }
        // search for the high values.
        while (lo2 <= hi2) {
            long mid = (hi2 + lo2) >>> 1;
            if (!excerpt.index(mid)) {
                if (mid > lo2)
                    excerpt.index(--mid);
                else
                    break;
            }
            int cmp = comparator.compare(excerpt);

            if (cmp <= 0) {
                lo2 = mid + 1;

            } else {
                hi2 = mid - 1;
            }
        }
        startEnd[0] = lo1; // inclusive
        startEnd[1] = lo2; // exclusive
    }
}
