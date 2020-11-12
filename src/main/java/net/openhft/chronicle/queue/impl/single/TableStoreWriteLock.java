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
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.StackTrace;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.AbstractTSQueueLock;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.Jvm.getProcessId;
import static net.openhft.chronicle.core.Jvm.warn;

public class TableStoreWriteLock extends AbstractTSQueueLock implements WriteLock {
    private static final String LOCK_KEY = "chronicle.write.lock";
    private static final long PID = getProcessId();
    private final long timeout;
    private Thread lockedByThread = null;
    private StackTrace lockedHere;

    public TableStoreWriteLock(final TableStore<?> tableStore, Supplier<TimingPauser> pauser, Long timeoutMs, final String lockKey) {
        super(lockKey, tableStore, pauser);
        timeout = timeoutMs;
    }

    public TableStoreWriteLock(final TableStore<?> tableStore, Supplier<TimingPauser> pauser, Long timeoutMs) {
        super(LOCK_KEY, tableStore, pauser);
        timeout = timeoutMs;
    }

    @Override
    public void lock() {
        throwExceptionIfClosed();

        assert checkNotAlreadyLocked();

        long value = 0;
        try {
            int i = 0;
            value = lock.getVolatileValue();
            while (!lock.compareAndSwapValue(UNLOCKED, PID)) {
                // add a tiny delay
                if (i++ > 1000 && Thread.interrupted())
                    throw new IllegalStateException("Interrupted for the lock file:" + path);
                pauser.pause(timeout, TimeUnit.MILLISECONDS);
                value = lock.getVolatileValue();
            }

            //noinspection ConstantConditions,AssertWithSideEffects
            assert (lockedByThread = Thread.currentThread()) != null
                    && (lockedHere = new StackTrace()) != null;

            // success
        } catch (TimeoutException e) {
            final String lockedBy = getLockedBy(value);
            final String warningMsg = "Couldn't acquire write lock " +
                    "after " + timeout + " ms " +
                    "for the lock file:" + path + ". " +
                    "Lock was held by " + lockedBy;
            if (dontRecoverLockTimeout)
                throw new UnrecoverableTimeoutException(new IllegalStateException(warningMsg));
            warn().on(getClass(), warningMsg + ". Unlocking forcibly");
            forceUnlock(value);
            // we should reset the pauser after a timeout exception
            pauser.reset();
            lock();
        } finally {
            pauser.reset();

        }
    }

    @NotNull
    protected String getLockedBy(long value) {
        final String lockedBy =
                value == Long.MIN_VALUE ? "unknown" :
                        value == PID ? "me"
                                : Long.toString((int) value);
        return lockedBy;
    }

    private boolean checkNotAlreadyLocked() {
        if (!locked())
            return true;
        if (lockedByThread == null)
            return true;
        if (lockedByThread == Thread.currentThread())
            throw new AssertionError("Lock is already acquired by current thread and is not reentrant - nested document context?", lockedHere);
        return true;
    }

    @Override
    public void unlock() {
        throwExceptionIfClosed();
        if (!lock.compareAndSwapValue(PID, UNLOCKED)) {
            long value = lock.getVolatileValue();
            if (value == UNLOCKED)
                warn().on(getClass(), "Write lock was unlocked by someone else! For the " +
                        "lock file:" + path);
            else
                warn().on(getClass(), "Write lock was locked by someone else! For the " +
                        "lock file:" + path + " " +
                        "by PID: " + getLockedBy(value));
        }
        //noinspection ConstantConditions,AssertWithSideEffects
        lockedByThread = null;
        lockedHere = null;
    }

    @Override
    public boolean locked() {
        throwExceptionIfClosed();
        return lock.getVolatileValue(UNLOCKED) != UNLOCKED;
    }
}
