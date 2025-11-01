/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/**
 * {@code NotReachedException} is thrown when an expected condition or state is not reached during the operation of a system or process.
 *
 * <p>This exception typically indicates that some required milestone or checkpoint in a process was not achieved, potentially due to
 * a failure or an unexpected state.
 */
public class NotReachedException extends IllegalStateException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new {@code NotReachedException} with the specified detail message.
     *
     * @param s the detail message explaining the reason the exception was thrown
     */
    public NotReachedException(final String s) {
        super(s);
    }
}
