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

public abstract class AbstractTSQueueLock extends AbstractCloseable implements Closeable {
    protected static final long PID = getProcessId();
    public static final long UNLOCKED = 1L << 63;
    protected final boolean dontRecoverLockTimeout = Jvm.getBoolean("queue.dont.recover.lock.timeout");

    protected final LongValue lock;
    protected final TimingPauser pauser;
    protected final File path;
    protected final TableStore tableStore;
    private final String lockKey;

    public AbstractTSQueueLock(final String lockKey, final TableStore<?> tableStore, final Supplier<TimingPauser> pauser) {
        this.tableStore = tableStore;
        this.lock = tableStore.doWithExclusiveLock(ts -> ts.acquireValueFor(lockKey));
        this.pauser = pauser.get();
        this.path = tableStore.file();
        this.lockKey = lockKey;
    }

    protected void performClose() {
        Closeable.closeQuietly(lock);
    }

    /**
     * will only force unlock if you give it the correct pid
     *
     * @param value
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
        if (pid==Jvm.getProcessId())
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
        long pid = 0;
        for (; ; ) {
            pid = this.lock.getVolatileValue();
            if (pid == UNLOCKED)
                return true;

            if (!Jvm.isProcessAlive(pid)) {
                if (Jvm.isDebugEnabled(this.getClass()))
                    Jvm.debug().on(this.getClass(), format("Forced unlocking `%s` in lock file:%s, as this was locked by: %d which is now dead",
                            lockKey, this.path, pid), new StackTrace("Forced unlock"));
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

    @Override
    protected boolean threadSafetyCheck(boolean isUsed) {
        // The lock is thread safe.
        return true;
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
