/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/**
 * The {@code IndexNotAvailableException} is thrown when an attempt is made to access an index
 * that is not available. This can occur when an index is expected but is either not present
 * or has not yet been generated.
 * <p>
 * This exception extends {@link IllegalStateException} to indicate that the current state
 * does not allow the requested index operation to be completed.
 */
public class IndexNotAvailableException extends IllegalStateException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs an {@code IndexNotAvailableException} with the specified detail message.
     *
     * @param message the detail message explaining why the index is not available.
     */
    public IndexNotAvailableException(String message) {
        super(message);
    }
}
