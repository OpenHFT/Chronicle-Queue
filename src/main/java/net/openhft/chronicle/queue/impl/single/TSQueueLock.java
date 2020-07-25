/*
 * Copyright 2014-2017 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

import static net.openhft.chronicle.core.Jvm.warn;

/**
 * Implements queue lock via TableStore mechanism.
 */
public class TSQueueLock extends AbstractTSQueueLock implements QueueLock {

    private static final String LOCK_KEY = "chronicle.queue.lock";
    private static final int PID = Jvm.getProcessId();
    private final long timeout;

    public TSQueueLock(final TableStore<?> tableStore, Supplier<TimingPauser> pauser, Long timeoutMs) {
        super(LOCK_KEY, tableStore, pauser);
        timeout = timeoutMs;
    }

    /**
     * Stores current PID to table store, and any other process trying to acquire lock will wait for
     * <code>chronicle.queue.lock.timeoutMS</code> millis (default is 30000) for the lock to be released, and if it is not
     * after timeout, throws {@link IllegalStateException}. Also the locking thread ID is stored in threadLocal field, so that only locking thread is
     * allowed to {@link net.openhft.chronicle.queue.ChronicleQueue#acquireAppender} while the queue is locked.
     */
    @Override
    public void acquireLock() {
        throwExceptionIfClosed();

        long tid = Thread.currentThread().getId();
        if (isLockHeldByCurrentThread(tid)) {
            return;
        }
        try {
            int count = 0;
            while (!lock.compareAndSwapValue(UNLOCKED, getLockValueFromTid(tid))) {
                if (count++ > 1000 && Thread.interrupted())
                    throw new IllegalStateException("Interrupted");
                pauser.pause(timeout, TimeUnit.MILLISECONDS);
            }
        } catch (TimeoutException e) {
            final long lockedByPID = lock.getVolatileValue();
            final String lockedBy = lockedByPID == PID ? "me" : Long.toString(lockedByPID);
            warn().on(getClass(), "Couldn't acquire lock after " + timeout + "ms for the lock file:"
                    + path + ", overriding the lock. Lock was held by PID " + lockedBy);
            forceUnlock();
            acquireLock();
        } finally {
            pauser.reset();

        }
    }

    private long getLockValueFromTid(long tid) {
        return tid << 32 | PID;
    }

    /**
     * checks if current thread holds lock. If not, it will wait for <code>chronicle.queue.lock.timeoutMS</code> millis for the lock to be released,
     * and if it is not after timeout, throws {@link IllegalStateException}.
     */
    @Override
    public void waitForLock() {
        throwExceptionIfClosed();

        long tid = Thread.currentThread().getId();
        if (isLockHeldByCurrentThread(tid))
            return;

        try {
            while (lock.getVolatileValue() != UNLOCKED) {
                if (Thread.interrupted())
                    throw new IllegalStateException("Interrupted");
                pauser.pause(timeout, TimeUnit.MILLISECONDS);
            }
        } catch (TimeoutException e) {
            warn().on(getClass(), "Queue lock is still held after " + timeout + "ms for the lock file:"
                    + path + ". Lock is held by PID " + lock.getVolatileValue() + ". Unlocking forcibly");
            forceUnlock();
        } catch (NullPointerException ex) {
            if (!tableStore.isClosed())
                throw ex;
            throw new IllegalStateException("The table store is closed!", ex);
        } finally {
            pauser.reset();
        }
    }

    /**
     * Checks if the lock is held by current thread and if so, releases it, removing entry from TableStore and clearing ThreadLocal state, allowing
     * anyone to proceed with {@link net.openhft.chronicle.queue.ChronicleQueue#acquireAppender}.
     */
    @Override
    public void unlock() {
        if (isClosed()) {
            Jvm.warn().on(getClass(), "Cannot unlock as already closed");
            return;
        }

        long tid = Thread.currentThread().getId();

        if (!lock.compareAndSwapValue(getLockValueFromTid(tid), UNLOCKED)) {
            warn().on(getClass(), "Queue lock was unlocked by someone else!");
        }
    }

    private boolean isLockHeldByCurrentThread(long tid) {
        return lock.getVolatileValue() == getLockValueFromTid(tid);
    }

}
