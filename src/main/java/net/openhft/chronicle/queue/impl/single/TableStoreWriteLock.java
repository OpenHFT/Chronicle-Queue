/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.AbstractTSQueueLock;
import net.openhft.chronicle.queue.impl.table.UnlockMode;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.Jvm.warn;

/**
 * The `TableStoreWriteLock` class provides an implementation of {@link WriteLock} using memory access primitives
 * to ensure safe concurrent access. It uses {@link AbstractTSQueueLock} to manage locking behavior.
 *
 * <p>It provides a non-reentrant locking mechanism that guarantees to acquire the lock or throw an exception
 * after a timeout. This class supports forceful unlocking depending on the {@link UnlockMode}.
 * The write lock is used to protect write operations in Chronicle Queue, ensuring that only one thread or process
 * can write at a time.
 */
public class TableStoreWriteLock extends AbstractTSQueueLock implements WriteLock, Closeable {
    private static final String STORE_LOCK_THREAD = "chronicle.store.lock.thread";
    private static final boolean storeLockThread = Jvm.getBoolean(STORE_LOCK_THREAD);
    public static final String APPEND_LOCK_KEY = "chronicle.append.lock";
    private static final String LOCK_KEY = "chronicle.write.lock";
    private final long timeout;
    private Thread lockedByThread = null;
    private StackTrace lockedHere;

    /**
     * Constructs a {@code TableStoreWriteLock} with a specified table store, pauser, timeout, and lock key.
     *
     * @param tableStore The {@link TableStore} object used for acquiring and managing lock state.
     * @param pauser A {@link Supplier} providing the {@link TimingPauser} instance for pausing between lock retries.
     * @param timeoutMs The timeout in milliseconds to wait before giving up on acquiring the lock.
     * @param lockKey The key used for identifying the lock.
     */
    public TableStoreWriteLock(final TableStore<?> tableStore, Supplier<TimingPauser> pauser, Long timeoutMs, final String lockKey) {
        super(lockKey, tableStore, pauser);
        timeout = timeoutMs;
    }

    /**
     * Constructs a {@code TableStoreWriteLock} with a specified table store, pauser, and timeout, using the default lock key.
     *
     * @param tableStore The {@link TableStore} object used for acquiring and managing lock state.
     * @param pauser A {@link Supplier} providing the {@link TimingPauser} instance for pausing between lock retries.
     * @param timeoutMs The timeout in milliseconds to wait before giving up on acquiring the lock.
     */
    public TableStoreWriteLock(final TableStore<?> tableStore, Supplier<TimingPauser> pauser, Long timeoutMs) {
        this(tableStore, pauser, timeoutMs, LOCK_KEY);
    }

    /**
     * Attempts to acquire the write lock. If the lock is already held by another thread/process, it will retry
     * until the timeout is reached. If the lock cannot be acquired within the timeout, it may force the unlock
     * based on the {@link UnlockMode}.
     *
     * @throws UnrecoverableTimeoutException if the lock could not be acquired and recovery is not allowed.
     */
    @Override
    public void lock() {
        throwExceptionIfClosed();

        assert checkNotAlreadyLocked();

        long currentLockValue = 0;
        TimingPauser tlPauser = pauser.get();
        try {
            currentLockValue = lock.getVolatileValue();

            while (!lock.compareAndSwapValue(UNLOCKED, PID)) {
                currentLockValue = lockGetCurrentLockValue(tlPauser);
            }
            checkStoreLockThread();

            // success
        } catch (TimeoutException e) {
            handleTimeoutEx(currentLockValue);
        } finally {
            tlPauser.reset();
        }
    }

    private long lockGetCurrentLockValue(TimingPauser tlPauser) throws TimeoutException {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedRuntimeException("Interrupted for the lock file:" + path);
        tlPauser.pause(timeout, TimeUnit.MILLISECONDS);
        return lock.getVolatileValue();
    }

    private void checkStoreLockThread() {
        if (storeLockThread) {
            lockedByThread = Thread.currentThread();
            lockedHere = new StackTrace();
        }
    }

    /**
     * Handles the case where lock acquisition times out. Depending on the {@link UnlockMode}, it may either
     * force the unlock or throw an {@link UnrecoverableTimeoutException}.
     *
     * @param currentLockValue The current lock value when the timeout occurred.
     */
    private void handleTimeoutEx(long currentLockValue) {
        final String lockedBy = getLockedBy(currentLockValue);
        final String warningMsg = lockHandleTimeoutExCreateWarningMessage(lockedBy);
        if (forceUnlockOnTimeoutWhen == UnlockMode.NEVER)
            throw new UnrecoverableTimeoutException(new IllegalStateException(warningMsg + UNLOCK_MAIN_MSG));
        else if (forceUnlockOnTimeoutWhen == UnlockMode.LOCKING_PROCESS_DEAD) {
            if (forceUnlockIfProcessIsDead())
                lock();
            else
                throw new UnrecoverableTimeoutException(new IllegalStateException(warningMsg + UNLOCK_MAIN_MSG));
        } else {
            warn().on(getClass(), warningMsg + UNLOCKING_FORCIBLY_MSG);
            forceUnlock(currentLockValue);
            lock();
        }
    }

    @NotNull
    private String lockHandleTimeoutExCreateWarningMessage(String lockedBy) {
        return "Couldn't acquire write lock " +
                "after " + timeout + " ms " +
                "for the lock file:" + path + ". " +
                "Lock was held by " + lockedBy;
    }

    /**
     * Returns the process/thread that holds the lock.
     *
     * @param value The current lock value.
     * @return A string representing the process holding the lock, or "me" if held by the current process.
     */
    @SuppressWarnings("deprecation")
    @NotNull
    protected String getLockedBy(long value) {
        String threadId = lockedByThread == null ? "unknown - " + STORE_LOCK_THREAD + " not set" : Long.toString(lockedByThread.getId());
        return value == Long.MIN_VALUE ? "unknown" :
                value == PID ? "current process (TID " + threadId + ")"
                        : Long.toString((int) value) + " (TID " + threadId + ")";
    }

    /**
     * Helper method to check if the lock is already held by the current thread.
     *
     * @return {@code true} if the lock is not already held by the current thread, otherwise throws an assertion error.
     */
    private boolean checkNotAlreadyLocked() {
        if (!locked())
            return true;
        if (lockedByThread == null)
            return true;
        if (lockedByThread == Thread.currentThread())
            throw new AssertionError("Lock is already acquired by current thread and is not reentrant - nested document context?", lockedHere);
        return true;
    }

    /**
     * Releases the write lock. If the lock is not held by the current process, a warning is logged.
     */
    @Override
    public void unlock() {
        throwExceptionIfClosed();
        if (!lock.compareAndSwapValue(PID, UNLOCKED)) {
            long value = lock.getVolatileValue();
            if (value == UNLOCKED)
                warn().on(getClass(), "Write lock was already unlocked. For the " +
                        "lock file:" + path);
            else
                warn().on(getClass(), "Write lock was locked by someone else! For the " +
                        "lock file:" + path + " " +
                        "by PID: " + getLockedBy(value));
        }
        lockedByThread = null;
        lockedHere = null;
    }

    /**
     * Checks whether the lock is currently held by any thread or process.
     *
     * @return {@code true} if the lock is held, {@code false} otherwise.
     */
    @Override
    public boolean locked() {
        throwExceptionIfClosed();
        return lock.getVolatileValue(UNLOCKED) != UNLOCKED;
    }

    /**
     * Forcefully unlocks the lock if it is held, without considering ownership.
     * This is primarily for internal use.
     */
    public void forceUnlock() {
        throwExceptionIfClosed();

        if (locked())
            forceUnlock(lockedBy());
    }
}
