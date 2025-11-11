/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.threads.TimingPauser;

import java.util.function.Supplier;

/**
 * The {@code AppendLock} class is a specialized implementation of {@link TableStoreWriteLock}.
 * It manages exclusive write access for appending data to a Chronicle Queue.
 * This class handles locking operations and includes mechanisms to forcefully unlock
 * if the process holding the lock is no longer running.
 */
public class AppendLock extends TableStoreWriteLock {

    /**
     * Constructs an {@code AppendLock} with the provided {@link TableStore}, {@link TimingPauser} supplier, and timeout.
     * The append lock ensures controlled access for appending operations.
     *
     * @param tableStore The {@link TableStore} associated with this lock.
     * @param pauser     The {@link TimingPauser} supplier used to manage lock acquisition retries.
     * @param timeoutMs  The maximum time, in milliseconds, to attempt acquiring the lock before timing out.
     */
    public AppendLock(TableStore<?> tableStore, Supplier<TimingPauser> pauser, Long timeoutMs) {
        super(tableStore, pauser, timeoutMs, TableStoreWriteLock.APPEND_LOCK_KEY);
    }

    /**
     * Attempts to forcefully unlock the lock only if the process that holds the lock is no longer running.
     * This is an override that disables warnings during force unlocking.
     *
     * @return {@code true} if the lock was successfully unlocked, {@code false} otherwise.
     */
    @Override
    public boolean forceUnlockIfProcessIsDead() {
        return super.forceUnlockIfProcessIsDead(false);
    }
}
