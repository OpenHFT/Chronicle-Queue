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
 * please don't use this interface as it's an internal implementation.
 */
public interface InternalAppender extends ExcerptAppender {

    /**
     * Append an excerpt at the specified index, if the index is a valid next index for the queue.
     * <p>
     * If the index is:
     * <dl>
     *     <dt>Greater than the next valid indices for the queue</dt>
     *     <dd>An {@link IllegalIndexException} is thrown</dd>
     *
     *     <dt>Less than or equal to the last index in the queue</dt>
     *     <dd>The method returns without modifying the queue</dd>
     * </dl>
     *
     * @param index index the index to append at
     * @param bytes bytes the contents of the excerpt to write
     * @throws IllegalIndexException if the index specified is larger than the valid next indices of the queue
     */
    void writeBytes(long index, BytesStore<?, ?> bytes);

}
