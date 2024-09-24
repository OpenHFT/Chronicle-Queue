/*
 * Copyright 2016-2020 chronicle.software
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

package net.openhft.chronicle.queue.impl.single.namedtailer;

import net.openhft.chronicle.core.values.LongValue;

import java.io.Closeable;

/**
 * The {@code IndexUpdater} interface is responsible for managing and updating the persistent index
 * of a named tailer. This ensures that the position of the tailer in the queue is saved and can be
 * retrieved or modified as needed.
 * <p>
 * Implementations of this interface should allow for the updating of the index in a persistent manner,
 * ensuring that the tailer's position in the queue is accurately maintained across operations.
 */
public interface IndexUpdater extends Closeable {

    /**
     * Updates the persistent index of the named tailer to the specified {@code index}.
     * <p>
     * This method sets the new index value, allowing the tailer to resume from the updated position
     * when reading from the queue.
     *
     * @param index the new index value to set
     */
    void update(long index);

    /**
     * Retrieves the current index value of the named tailer.
     * <p>
     * The returned {@link LongValue} represents the position of the tailer in the queue.
     *
     * @return the current index value of the named tailer
     */
    LongValue index();

}
