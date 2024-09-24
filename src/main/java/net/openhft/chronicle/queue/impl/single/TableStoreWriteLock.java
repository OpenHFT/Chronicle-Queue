/*
 * Copyright 2014-2020 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.StackTrace;
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

import static net.openhft.chronicle.assertions.AssertUtil.SKIP_ASSERTIONS;
import static net.openhft.chronicle.core.Jvm.warn;

/**
 * The `TableStoreWriteLock` class provides an implementation of {@link WriteLock} using memory access primitives
 * to ensure safe concurrent access. It uses {@link AbstractTSQueueLock} to manage locking behavior.
 *
 * <p>It provides a non-reentrant locking mechanism that guarantees to acquire the lock or throw an exception
 * after a timeout. This class supports forceful unlocking depending on the {@link UnlockMode}.
 * The write lock is used to protect write operations in Chronicle Queue, ensuring that only one thread or process
 * can write at a time.</p>
 */
public class TableStoreWriteLock extends AbstractTSQueueLock implements WriteLock {
    public static final String APPEND_LOCK_KEY = "chronicle.append.lock"; // Key for append locks
    private static final String LOCK_KEY = "chronicle.write.lock"; // Default key for write locks
    private final long timeout; // Lock timeout in milliseconds
    private Thread lockedByThread = null; // Thread currently holding the lock
    private StackTrace lockedHere; // Stack trace where the lock was acquired

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
        this(tableStore, pauser, timeoutMs, LOCK_KEY); // Calls the constructor with default lock key
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
        assert checkNotAlreadyLocked(); // Ensures that the lock is not already held by the current thread

        long currentLockValue = 0;
        TimingPauser tlPauser = pauser.get();
        try {
            currentLockValue = lock.getVolatileValue();
            while (!lock.compareAndSwapValue(UNLOCKED, PID)) { // Try to acquire the lock
                currentLockValue = lockGetCurrentLockValue(tlPauser); // Retry on failure
            }

            lockAssertPostConditions(); // Post-lock assertions
        } catch (TimeoutException e) {
            handleTimeoutEx(currentLockValue); // Handle lock acquisition timeout
        } finally {
            tlPauser.reset(); // Reset the pauser
        }
    }

    private long lockGetCurrentLockValue(TimingPauser tlPauser) throws TimeoutException {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedRuntimeException("Interrupted for the lock file:" + path);
        tlPauser.pause(timeout, TimeUnit.MILLISECONDS);
        return lock.getVolatileValue();
    }

    /**
     * Asserts that the lock has been acquired successfully by the current thread.
     */
    private void lockAssertPostConditions() {
        //noinspection ConstantConditions,AssertWithSideEffects
        assert SKIP_ASSERTIONS ||
                ((lockedByThread = Thread.currentThread()) != null && (lockedHere = new StackTrace()) != null);
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
                lock(); // Retry locking
            else
                throw new UnrecoverableTimeoutException(new IllegalStateException(warningMsg + UNLOCK_MAIN_MSG));
        } else {
            warn().on(getClass(), warningMsg + UNLOCKING_FORCIBLY_MSG);
            forceUnlock(currentLockValue); // Force unlock
            lock(); // Retry locking
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
    @NotNull
    protected String getLockedBy(long value) {
        return value == Long.MIN_VALUE ? "unknown" :
                value == PID ? "me"
                        : Long.toString((int) value);
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
