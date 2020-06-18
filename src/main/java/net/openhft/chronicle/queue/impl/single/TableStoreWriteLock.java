/*
 * Copyright 2014-2018 Chronicle Software
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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.AbstractTSQueueLock;
import net.openhft.chronicle.threads.TimingPauser;

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

    public TableStoreWriteLock(final TableStore<?> tableStore, Supplier<TimingPauser> pauser, Long timeoutMs) {
        super(LOCK_KEY, tableStore, pauser);
        timeout = timeoutMs;
    }

    @Override
    public void lock() {
        throwExceptionIfClosed();

        assert checkNotAlreadyLocked();
        try {
            int i = 0;
            while (!lock.compareAndSwapValue(UNLOCKED, PID)) {
                // add a tiny delay
                Jvm.safepoint();
                if (i++ > 1000 && Thread.interrupted())
                    throw new IllegalStateException("Interrupted for the lock file:" + path);
                pauser.pause(timeout, TimeUnit.MILLISECONDS);
            }

            //noinspection ConstantConditions,AssertWithSideEffects
            assert (lockedByThread = Thread.currentThread()) != null;

            // success
        } catch (TimeoutException e) {
            final long lockedByPID = lock.getVolatileValue(Long.MIN_VALUE);
            final String lockedBy =
                    lockedByPID == Long.MIN_VALUE ? "unknown" :
                            lockedByPID == PID ? "me"
                                    : Long.toString(lockedByPID);
            warn().on(getClass(), "Couldn't acquire write lock after " + timeout
                    + "ms for the lock file:" + path + ", overriding the lock. Lock was held by " + lockedBy);
            forceUnlock();
            // we should reset the pauser after a timeout exception
            pauser.reset();
            lock();
        } finally {
            pauser.reset();

        }
    }

    private boolean checkNotAlreadyLocked() {
        if (lockedByThread == null)
            return true;
        if (lockedByThread == Thread.currentThread())
            throw new AssertionError("Lock is already acquired by current thread and is not reentrant - nested document context?");
        return true;
    }

    @Override
    public void unlock() {
        if (!lock.compareAndSwapValue(PID, UNLOCKED)) {
            warn().on(getClass(), "Write lock was unlocked by someone else! For the lock file:" + path);
        }
        //noinspection ConstantConditions,AssertWithSideEffects
        assert (lockedByThread = null) == null;
    }

    @Override
    public boolean locked() {
        return lock.getVolatileValue(UNLOCKED) != UNLOCKED;
    }
}
