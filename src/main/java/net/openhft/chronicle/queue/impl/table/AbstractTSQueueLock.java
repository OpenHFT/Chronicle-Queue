/*
 * Copyright 2014-2020 chronicle.software
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
 * Implements a lock using {@link LongValue} and primitives such as CAS.
 * <p>
 * WARNING: the default behaviour (see also {@code queue.dont.recover.lock.timeout} system property) is
 * for a timed-out lock to be overridden.
 */
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
    protected final TableStore tableStore;
    private final String lockKey;

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
            // TODO: x.24 change default to UnlockMode.LOCKING_PROCESS_DEAD
            forceUnlockOnTimeoutWhen = UnlockMode.valueOf(Jvm.getProperty("queue.force.unlock.mode", UnlockMode.ALWAYS.name()));
        }

        disableThreadSafetyCheck(true);
    }

    protected void performClose() {
        Closeable.closeQuietly(lock);
    }

    /**
     * will only force unlock if you give it the correct pid
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
        long pid;
        for (; ; ) {
            pid = this.lock.getVolatileValue();
            if (pid == UNLOCKED)
                return true;

            // mask off thread (if used)
            int realPid = (int) pid;
            if (!Jvm.isProcessAlive(realPid)) {
                if (Jvm.isDebugEnabled(this.getClass()))
                    Jvm.debug().on(this.getClass(), format("Forced unlocking `%s` in lock file:%s, as this was locked by: %d which is now dead",
                            lockKey, this.path, realPid), new StackTrace("Forced unlock"));
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
     * @return the pid that had the locked or the returns UNLOCKED if it is not locked
     */
    public long lockedBy() {
        return lock.getVolatileValue();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "lock=" + lock +
                ", path=" + path +
                ", lockKey='" + lockKey + '\'' +
                '}';
    }
}
