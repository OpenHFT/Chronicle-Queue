/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

/**
 * {@code NotComparableException} is thrown by a binary search comparator when the value being searched for
 * is not present in the current entry.
 *
 * <p>This exception is expected to be rare during a binary search operation. However, when it occurs,
 * it can significantly reduce the performance of the binary search due to the need for additional comparisons
 * or fallback logic.</p>
 *
 * <p>This exception is a singleton, with a single instance available via the {@link #INSTANCE} field.</p>
 */
public final class NotComparableException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    /**
     * Singleton instance of {@code NotComparableException}.
     */
    public static final NotComparableException INSTANCE = new NotComparableException();

    /**
     * Private constructor to enforce singleton usage via {@link #INSTANCE}.
     */
    private NotComparableException() {
        // No additional initialization required
    }
}
