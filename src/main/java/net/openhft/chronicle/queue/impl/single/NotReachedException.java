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
 * {@code NotReachedException} is thrown when an expected condition or state is not reached during the operation of a system or process.
 *
 * <p>This exception typically indicates that some required milestone or checkpoint in a process was not achieved, potentially due to
 * a failure or an unexpected state.</p>
 */
public class NotReachedException extends IllegalStateException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new {@code NotReachedException} with the specified detail message.
     *
     * @param s the detail message explaining the reason the exception was thrown
     */
    public NotReachedException(final String s) {
        super(s);
    }
}
