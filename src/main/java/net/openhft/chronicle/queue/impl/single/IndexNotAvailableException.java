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
 * The {@code IndexNotAvailableException} is thrown when an attempt is made to access an index
 * that is not available. This can occur when an index is expected but is either not present
 * or has not yet been generated.
 * <p>
 * This exception extends {@link IllegalStateException} to indicate that the current state
 * does not allow the requested index operation to be completed.
 */
public class IndexNotAvailableException extends IllegalStateException {

    // Serial version ID for ensuring compatibility during serialization.
    private static final long serialVersionUID = 0L;

    /**
     * Constructs an {@code IndexNotAvailableException} with the specified detail message.
     *
     * @param message the detail message explaining why the index is not available.
     */
    public IndexNotAvailableException(String message) {
        super(message);
    }
}
