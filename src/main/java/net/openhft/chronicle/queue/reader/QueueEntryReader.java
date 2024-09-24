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

package net.openhft.chronicle.queue.reader;

/**
 * Interface for reading and processing entries from a queue.
 * <p>Implementations of this interface are responsible for reading the next available entry
 * from the queue and processing it as necessary.</p>
 */
public interface QueueEntryReader {

    /**
     * Reads and processes the next entry from the queue.
     *
     * @return {@code true} if there was an entry to read and process, {@code false} if the end of the queue has been reached
     */
    boolean read();
}
