/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.queue.impl.TableStore;

/**
 * Enum representing the action to take when lock acquisition times out in a {@link TableStore}.
 * It defines different strategies for handling lock acquisition failures.
 */
public enum UnlockMode {
    /**
     * Always force unlock and re-acquire the lock, regardless of the state of the locking process.
     */
    ALWAYS,
    /**
     * Never force unlock. Instead, an exception will be thrown if lock acquisition times out.
     */
    NEVER,
    /**
     * Force unlock and re-acquire the lock only if the process holding the lock is no longer alive.
     * If the locking process is still running, an exception is thrown.
     */
    LOCKING_PROCESS_DEAD
}
