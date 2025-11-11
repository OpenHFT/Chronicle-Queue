/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import static java.lang.String.format;

/**
 * The {@code IllegalIndexException} is thrown when an index provided to a method or operation
 * is after the next index in the queue, violating the queue's index boundaries.
 * <p>
 * This exception extends {@link IllegalArgumentException} and provides a detailed message
 * including the provided index and the last valid index in the queue.
 */
public class IllegalIndexException extends IllegalArgumentException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs an {@code IllegalIndexException} with a formatted message indicating
     * the provided index and the last valid index in the queue.
     *
     * @param providedIndex The index that was provided, which is after the next valid index.
     * @param lastIndex     The last valid index in the queue.
     */
    public IllegalIndexException(long providedIndex, long lastIndex) {
        // Create an exception message with hex formatting for both indices
        super(format("Index provided is after the next index in the queue, provided index = %x, last index in queue = %x", providedIndex, lastIndex));
    }
}
