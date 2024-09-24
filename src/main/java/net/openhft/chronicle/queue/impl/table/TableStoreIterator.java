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

package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.wire.ValueIn;

/**
 * The {@code TableStoreIterator} interface provides a method for iterating over key-value pairs in a {@link TableStore}.
 * The iteration is performed by accepting an accumulator and applying the key-value pair to it.
 *
 * @param <A> The type of the accumulator used during the iteration.
 */
public interface TableStoreIterator<A> {

    /**
     * Accepts a key-value pair from the table store and applies it to the provided accumulator.
     *
     * @param accumulator The accumulator to which the key-value pair is applied.
     * @param key         The key from the table store.
     * @param value       The value associated with the key, represented by {@link ValueIn}.
     */
    void accept(A accumulator, CharSequence key, ValueIn value);
}
