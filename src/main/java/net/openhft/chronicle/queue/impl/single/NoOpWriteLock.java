/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.util.IgnoresEverything;

import java.util.function.LongConsumer;

/**
 * {@link WriteLock} implementation that performs no locking and reports success.
 * <p>
 * This is used when a queue instance is known to be single threaded or when locking is
 * enforced by some other mechanism. All methods are effectively no-ops but return values
 * are chosen so that callers treat the lock as always acquired by the current process.
 */
@SuppressWarnings({"deprecation", "removal"})
public class NoOpWriteLock implements WriteLock, IgnoresEverything {

    @Override
    public void lock() {
    }

    @Override
    public void unlock() {
    }

    @Override
    public void close() {
    }

    @Override
    public boolean forceUnlockIfProcessIsDead() {
        return true;
    }

    @Override
    public boolean isLockedByCurrentProcess(LongConsumer notCurrentProcessConsumer) {
        return true;
    }
}
