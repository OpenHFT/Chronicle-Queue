/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.util.IgnoresEverything;
import org.jetbrains.annotations.NotNull;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * {@code NoOpCondition} is a no-operation implementation of the {@link Condition} interface.
 * This condition is always true and does not block, signal, or modify any thread state.
 *
 * <p>All operations on this condition return immediately without performing any blocking
 * or waiting behavior, effectively serving as a placeholder or dummy condition.
 */
public final class NoOpCondition implements Condition, IgnoresEverything {

    /**
     * Singleton instance of {@code NoOpCondition}, as it has no mutable state and can be reused.
     */
    public static final NoOpCondition INSTANCE = new NoOpCondition();

    /**
     * Private constructor to enforce singleton usage through {@link #INSTANCE}.
     */
    private NoOpCondition() { }

    /**
     * Does nothing, returns immediately.
     */
    @Override
    public void await() {
    }

    /**
     * Does nothing, returns immediately.
     */
    @Override
    public void awaitUninterruptibly() {
    }

    /**
     * Returns the input nanosecond duration without any delay or action.
     *
     * @param nanosTimeout The timeout in nanoseconds.
     * @return The same input nanosecond value.
     */
    @Override
    public long awaitNanos(long nanosTimeout) {
        return nanosTimeout;
    }

    /**
     * Returns {@code true} without blocking.
     *
     * @param l The maximum time to wait.
     * @param timeUnit The time unit of the {@code l} argument.
     * @return Always returns {@code true}.
     * @throws InterruptedException This method does not throw an exception.
     */
    @Override
    public boolean await(long l, TimeUnit timeUnit) throws InterruptedException {
        return true;
    }

    /**
     * Returns {@code true} without waiting for the given date.
     *
     * @param date The deadline by which waiting should end.
     * @return Always returns {@code true}.
     */
    @Override
    public boolean awaitUntil(@NotNull Date date) {
        return true;
    }

    /**
     * Does nothing, returns immediately.
     */
    @Override
    public void signal() {
    }

    /**
     * Does nothing, returns immediately.
     */
    @Override
    public void signalAll() {
    }
}
