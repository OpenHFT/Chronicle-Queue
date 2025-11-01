/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.reader.comparator;

import net.openhft.chronicle.queue.reader.Reader;
import net.openhft.chronicle.wire.Wire;

import java.util.Comparator;
import java.util.function.Consumer;

/**
 * Interface for implementing a comparator used in binary search operations within the {@link Reader}.
 * <p>This interface extends {@link Comparator} to compare {@link Wire} objects, and {@link Consumer} to allow the comparator
 * to configure itself using a {@link Reader} instance.
 */
public interface BinarySearchComparator extends Comparator<Wire>, Consumer<Reader> {

    /**
     * Provides the key used in the binary search, represented as a {@link Wire}.
     *
     * @return The {@link Wire} object representing the search key
     */
    Wire wireKey();
}
