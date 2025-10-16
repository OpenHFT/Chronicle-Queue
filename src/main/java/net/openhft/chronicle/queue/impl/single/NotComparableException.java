/*
 * Copyright 2016-2025 chronicle.software
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
 * Thrown by a binary search comparator when the value we're searching on is not present in the current
 * entry.
 * <p>
 * The assumption is that this occurs rarely. If it does, it will significantly reduce the performance
 * of the binary search.
 */
public final class NotComparableException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    public static final NotComparableException INSTANCE = new NotComparableException();

    private NotComparableException() {
    }
}
