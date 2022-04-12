/*
 * Copyright 2014 Higher Frequency Trading
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

package net.openhft.chronicle.queue.stateless.bytes;

import net.openhft.chronicle.bytes.Bytes;
import org.jetbrains.annotations.NotNull;

/**
 * todo : currently work in process
 * <p>
 * Created by Rob Austin
 */
public interface StatelessRawBytesTailer {

    /**
     * reads bytes for a given index into the buffer
     *
     * @param index the index to read the bytes at
     * @return null will be returned if their is not an Except at this index
     */
    @NotNull
    Bytes<?> readExcept(long index);

    /**
     * reads bytes for a given index into the buffer
     *
     * @param index the index to read the bytes at
     * @param using the buffer to use if its large enough, otherwise a new buffer will be created
     * @return null will be returned if their is not an Except at this index
     */
    boolean readExcept(long index, Bytes<?> using);

    /**
     * @return the last index
     */
    long lastWrittenIndex();

    /**
     * @param l the index to get
     * @return true if the index is valid and its not just padding
     */
    boolean index(long l);
}
