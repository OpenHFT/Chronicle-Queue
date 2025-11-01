/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.table;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.threads.TimingPauser;

import java.io.File;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static java.lang.String.format;
import static net.openhft.chronicle.core.Jvm.getProcessId;

/**
 * Implements a lock using {@link LongValue} and primitives such as Compare-And-Swap (CAS).
 * <p>
 * The lock is associated with a specific table store and can be used to ensure exclusive access
 * to resources. The locking mechanism allows for forced unlocking in case of a dead process,
 * with an optional recovery feature based on the {@code queue.force.unlock.mode} system property.
 * <p>
 * WARNING: By default, the lock can be overridden if it times out, but this behavior can be controlled
 * with the system property {@code queue.force.unlock.mode}.
 */
@SuppressWarnings("this-escape")
public abstract class AbstractTSQueueLock extends AbstractCloseable implements Closeable {
    protected static final String UNLOCK_MAIN_MSG = ". You can manually unlock with net.openhft.chronicle.queue.main.UnlockMain";
    protected static final String UNLOCKING_FORCIBLY_MSG = ". Unlocking forcibly. Note that this feature is designed to recover " +
            "if another process died while holding a lock. If the other process is still alive, you may see queue corruption.";
    protected static final long PID = getProcessId();
    public static final long UNLOCKED = 1L << 63;
    protected final UnlockMode forceUnlockOnTimeoutWhen;

    protected final LongValue lock;
    protected final ThreadLocal<TimingPauser> pauser;
    protected final File path;
    protected final TableStore<?> tableStore;
    private final String lockKey;

    /**
     * Constructor for creating an AbstractTSQueueLock.
     *
     * @param lockKey        The unique key associated with this lock.
     * @param tableStore     The table store this lock will manage.
     * @param pauserSupplier A supplier for creating a {@link TimingPauser}.
     */
    public AbstractTSQueueLock(final String lockKey, final TableStore<?> tableStore, final Supplier<TimingPauser> pauserSupplier) {
        this.tableStore = tableStore;
        this.lock = tableStore.doWithExclusiveLock(ts -> ts.acquireValueFor(lockKey));
        this.pauser = ThreadLocal.withInitial(pauserSupplier);
        this.path = tableStore.file();
        this.lockKey = lockKey;

        if (Jvm.getProperty("queue.dont.recover.lock.timeout") != null) {
            throw new IllegalStateException("queue.dont.recover.lock.timeout property is no longer supported. Use queue.force.unlock.mode instead");
        }
        forceUnlockOnTimeoutWhen = UnlockMode.valueOf(Jvm.getProperty("queue.force.unlock.mode", UnlockMode.LOCKING_PROCESS_DEAD.name()).toUpperCase());

        singleThreadedCheckDisabled(true);
    }

    /**
     * Performs cleanup and releases resources when the lock is closed.
     */
    protected void performClose() {
        Closeable.closeQuietly(lock);
    }

    /**
     * Forces the lock to be unlocked, only if the current process owns the lock.
     *
     * @param value The value representing the current lock holder (process ID and thread).
     */
    protected void forceUnlock(long value) {
        boolean unlocked = lock.compareAndSwapValue(value, UNLOCKED);
        Jvm.warn().on(getClass(), "" +
                        "Forced unlock for the " +
                        "lock file:" + path + ", " +
                        "lockKey: " + lockKey + ", " +
                        "unlocked: " + unlocked,
                new StackTrace("Forced unlock"));
    }

    /**
     * Checks if the lock is held by the current process.
     *
     * @param notCurrentProcessConsumer A consumer that will be called if the lock is not held by the current process.
     * @return {@code true} if the lock is held by the current process, {@code false} otherwise.
     */
    public boolean isLockedByCurrentProcess(LongConsumer notCurrentProcessConsumer) {
        final long pid = this.lock.getVolatileValue();
        // mask off thread (if used)
        int realPid = (int) pid;
        if (realPid == PID)
            return true;
        notCurrentProcessConsumer.accept(pid);
        return false;
    }

    /**
     * forces an unlock only if the process that currently holds the table store lock is no-longer running
     *
     * @return {@code true} if the lock was already unlocked, It will not release the lock if it is held by this process
     * or the process that was holding the lock is no longer running (and we were able to unlock).
     * Otherwise {@code false} is returned if the lock is held by this process or another live process.
     */
    public boolean forceUnlockIfProcessIsDead() {
        return forceUnlockIfProcessIsDead(true);
    }

    /**
     * Forces an unlock if the process holding the lock is no longer alive.
     *
     * @param warn If {@code true}, log a warning message; otherwise, log a debug message.
     * @return {@code true} if the lock was unlocked, otherwise {@code false}.
     */
    protected boolean forceUnlockIfProcessIsDead(boolean warn) {
        long pid;
        for (; ; ) {
            pid = this.lock.getVolatileValue();
            if (pid == UNLOCKED)
                return true;

            // mask off thread (if used)
            int realPid = (int) pid;
            if (!Jvm.isProcessAlive(realPid)) {
                final String message = format("Forced unlocking `%s` in lock file:%s, as this was locked by: %d which is now dead",
                        lockKey, this.path, realPid);
                if (warn) {
                    Jvm.warn().on(this.getClass(), message);
                } else {
                    Jvm.debug().on(this.getClass(), message);
                }
                if (lock.compareAndSwapValue(pid, UNLOCKED))
                    return true;
            } else {
                break;
            }
        }
        if (Jvm.isDebugEnabled(this.getClass()))
            // don't make this a WARN as this method should only unlock if process is dead or current process.
            Jvm.debug().on(this.getClass(), format("Unable to release the lock=%s in the table store file=%s " +
                    "as it is being held by pid=%d, and this process is still running.", lockKey, path, pid));
        return false;
    }

    /**
     * Gets the process ID (PID) that currently holds the lock.
     *
     * @return The process ID holding the lock, or {@code UNLOCKED} if it is not locked.
     */
    public long lockedBy() {
        return lock.getVolatileValue();
    }

    /**
     * Provides a string representation of the lock, including the lock key and path.
     *
     * @return A string describing the lock.
     */
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "lock=" + lock +
                ", path=" + path +
                ", lockKey='" + lockKey + '\'' +
                '}';
    }
}
