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

    // Message displayed when manual unlocking is suggested
    protected static final String UNLOCK_MAIN_MSG = ". You can manually unlock with net.openhft.chronicle.queue.main.UnlockMain";

    // Message for forcibly unlocking the lock
    protected static final String UNLOCKING_FORCIBLY_MSG = ". Unlocking forcibly. Note that this feature is designed to recover " +
            "if another process died while holding a lock. If the other process is still alive, you may see queue corruption.";

    // Process ID of the current process
    protected static final long PID = getProcessId();

    // Represents an unlocked state
    public static final long UNLOCKED = 1L << 63;

    // The mode for handling forced unlocking
    protected final UnlockMode forceUnlockOnTimeoutWhen;

    // Lock associated with the table store
    protected final LongValue lock;

    // Pauser to manage timing and retries
    protected final ThreadLocal<TimingPauser> pauser;

    // The file path of the table store
    protected final File path;

    // The table store this lock is associated with
    protected final TableStore<?> tableStore;

    // A unique key representing this lock
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

        final boolean dontRecoverLockTimeout = Jvm.getBoolean("queue.dont.recover.lock.timeout");
        if (dontRecoverLockTimeout) {
            forceUnlockOnTimeoutWhen = UnlockMode.NEVER;
            Jvm.warn().on(getClass(), "queue.dont.recover.lock.timeout property is deprecated and will be removed in a future version. " +
                    "Use queue.force.unlock.mode=NEVER instead");
        } else {
            forceUnlockOnTimeoutWhen = UnlockMode.valueOf(Jvm.getProperty("queue.force.unlock.mode", UnlockMode.LOCKING_PROCESS_DEAD.name()).toUpperCase());
        }

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
        // Mask off the thread part, if present
        int realPid = (int) pid;
        if (realPid == PID)
            return true;
        notCurrentProcessConsumer.accept(pid);
        return false;
    }

    /**
     * Forces an unlock if the process that holds the lock is no longer running.
     *
     * @return {@code true} if the lock was unlocked, otherwise {@code false}.
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

            // Mask off thread (if used)
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
            } else
                break;
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
