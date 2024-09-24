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

package net.openhft.chronicle.queue.reader.comparator;

import net.openhft.chronicle.queue.reader.Reader;
import net.openhft.chronicle.wire.Wire;

import java.util.Comparator;
import java.util.function.Consumer;

/**
 * Interface for implementing a comparator used in binary search operations within the {@link Reader}.
 * <p>This interface extends {@link Comparator} to compare {@link Wire} objects, and {@link Consumer} to allow the comparator
 * to configure itself using a {@link Reader} instance.</p>
 */
public interface BinarySearchComparator extends Comparator<Wire>, Consumer<Reader> {

    /**
     * Provides the key used in the binary search, represented as a {@link Wire}.
     *
     * @return The {@link Wire} object representing the search key
     */
    Wire wireKey();
}
