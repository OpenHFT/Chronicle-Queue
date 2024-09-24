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

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.ExcerptAppender;

/**
 * This interface, {@code InternalAppender}, extends the {@link ExcerptAppender} and provides
 * additional functionality for appending entries at specific indices within a queue.
 * <p>
 * <strong>Note:</strong> This is an internal interface and should not be used externally
 * as it is subject to changes without notice.
 */
public interface InternalAppender extends ExcerptAppender {

    /**
     * Appends an excerpt at the specified index, provided that the index is the next valid one
     * for the queue.
     * <p>
     * Behavior for various index scenarios:
     * <ul>
     *     <li>If the provided index is <strong>greater</strong> than the next valid index for the queue,
     *     an {@link IllegalIndexException} will be thrown.</li>
     *     <li>If the provided index is <strong>less than or equal</strong> to the last index in the queue,
     *     the method will return without modifying the queue.</li>
     * </ul>
     *
     * @param index The index at which to append the excerpt.
     * @param bytes The content to write at the specified index.
     * @throws IllegalIndexException if the specified index is beyond the next valid index for the queue.
     */
    void writeBytes(long index, BytesStore<?, ?> bytes);
}
