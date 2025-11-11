/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

/**
 * {@code NotComparableException} is thrown by a binary search comparator when the value being searched for
 * is not present in the current entry.
 *
 * <p>This exception is expected to be rare during a binary search operation. However, when it occurs,
 * it can significantly reduce the performance of the binary search due to the need for additional comparisons
 * or fallback logic.
 *
 * <p>This exception is a singleton, with a single instance available via the {@link #INSTANCE} field.
 */
public final class NotComparableException extends RuntimeException {
    private static final long serialVersionUID = 0L;

    /**
     * Singleton instance of {@code NotComparableException}.
     */
    public static final NotComparableException INSTANCE = new NotComparableException();

    /**
     * Private constructor to enforce singleton usage via {@link #INSTANCE}.
     */
    private NotComparableException() {
    }
}
