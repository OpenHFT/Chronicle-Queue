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

import static java.lang.String.format;

/**
 * The {@code IllegalIndexException} is thrown when an index provided to a method or operation
 * is after the next index in the queue, violating the queue's index boundaries.
 * <p>
 * This exception extends {@link IllegalArgumentException} and provides a detailed message
 * including the provided index and the last valid index in the queue.
 */
public class IllegalIndexException extends IllegalArgumentException {
    private static final long serialVersionUID = 0L; // Unique ID for serialization

    /**
     * Constructs an {@code IllegalIndexException} with a formatted message indicating
     * the provided index and the last valid index in the queue.
     *
     * @param providedIndex The index that was provided, which is after the next valid index.
     * @param lastIndex     The last valid index in the queue.
     */
    public IllegalIndexException(long providedIndex, long lastIndex) {
        // Create an exception message with hex formatting for both indices
        super(format("Index provided is after the next index in the queue, provided index = %x, last index in queue = %x", providedIndex, lastIndex));
    }
}
