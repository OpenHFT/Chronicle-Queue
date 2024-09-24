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

package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.io.ManagedCloseable;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.table.Metadata;
import net.openhft.chronicle.queue.impl.table.TableStoreIterator;

import java.util.function.Function;

/**
 * The {@code TableStore} interface provides a way to manage and access values within a mapped file, offering atomic operations
 * and support for concurrent access across multiple threads and processes.
 *
 * <p>The table store maintains mappings of keys to {@link LongValue}s and provides mechanisms for atomic updates,
 * acquiring exclusive file-system locks, and iterating over keys.</p>
 *
 * @param <T> The type of metadata associated with the table store, extending {@link Metadata}.
 */
public interface TableStore<T extends Metadata> extends CommonStore, ManagedCloseable {

    /**
     * Acquires a {@link LongValue} mapped to the underlying file, providing atomic operations on the value shared
     * across threads and/or JVMs.
     *
     * <p>This method may lead to concurrent threads or processes acquiring {@link LongValue}s pointing to different
     * fields in the underlying file. To ensure exclusive access and prevent data corruption, it is recommended to wrap
     * this call within {@link #doWithExclusiveLock(Function)}.</p>
     *
     * <p>If the value is not found, a new {@link LongValue} is created with {@link Long#MIN_VALUE} as the default.
     * To specify another default value, use {@link #acquireValueFor(CharSequence, long)}.</p>
     *
     * @param key the key of the value
     * @return {@link LongValue} object pointing to a particular location in the mapped file
     */
    default LongValue acquireValueFor(CharSequence key) {
        return acquireValueFor(key, Long.MIN_VALUE);
    }

    /**
     * Acquires a {@link LongValue} for the given key, initializing it with the specified default value if not found.
     *
     * @param key          the key for which to acquire the {@link LongValue}
     * @param defaultValue the default value to initialize with if the key is not found
     * @return {@link LongValue} object pointing to the corresponding location in the mapped file
     */
    LongValue acquireValueFor(CharSequence key, long defaultValue);

    /**
     * Iterates over each key in the table store and applies the given {@link TableStoreIterator} on it.
     *
     * @param <A>          the type of the accumulator
     * @param accumulator  the accumulator to collect results
     * @param tsIterator   the iterator to process each key
     */
    <A> void forEachKey(A accumulator, TableStoreIterator<A> tsIterator);

    /**
     * Acquires an exclusive file-system level lock on the underlying file to prevent concurrent access from multiple processes.
     *
     * <p>This ensures that all operations within the provided code block are executed atomically, providing thread/process safety.
     * It is recommended to use this when acquiring values for the first time to avoid unpredictable results due to concurrent access.</p>
     *
     * @param code code block to execute using the locked table store
     * @param <R>  result type of the code block
     * @return result of the code block execution
     */
    <R> R doWithExclusiveLock(Function<TableStore<T>, ? extends R> code);

    /**
     * Retrieves the metadata associated with this table store.
     *
     * @return metadata of type {@link T}
     */
    T metadata();

    /**
     * Checks if the table store is in read-only mode.
     *
     * @return {@code true} if the table store is read-only, {@code false} otherwise
     */
    default boolean readOnly() {
        return false;
    }
}
