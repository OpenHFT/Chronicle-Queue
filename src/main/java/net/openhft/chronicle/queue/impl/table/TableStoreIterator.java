/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.queue.impl.TableStore;
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
