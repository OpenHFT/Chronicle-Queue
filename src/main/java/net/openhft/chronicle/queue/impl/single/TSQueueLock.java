/*
 * Copyright 2014-2017 Higher Frequency Trading
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
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.threads.InterruptedRuntimeException;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.AbstractTSQueueLock;
import net.openhft.chronicle.queue.impl.table.UnlockMode;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.Jvm.warn;

/**
 * Implements queue lock via TableStore mechanism.
 */
public class TSQueueLock extends AbstractTSQueueLock implements QueueLock {

    private static final String LOCK_KEY = "chronicle.queue.lock";
    private final long timeout;

    public TSQueueLock(final TableStore<?> tableStore, Supplier<TimingPauser> pauser, long timeoutMs) {
        super(LOCK_KEY, tableStore, pauser);
        timeout = timeoutMs;
    }

    /**
     * Stores current TID and PID to table store, and any other thread trying to acquire lock will wait for
     * <code>chronicle.queue.lock.timeoutMS</code> millis (default is 30000) for the lock to be released, and if it is not
     * able to lock, *overrides the lock*.
     */
    @Override
    public void acquireLock() {
        throwExceptionIfClosed();

        long tid = Thread.currentThread().getId();
        if (isLockHeldByCurrentThread(tid)) {
            return;
        }
        int count = 0;
        long lockValueFromTid = getLockValueFromTid(tid);
        long value = lock.getVolatileValue();
        Pauser tlPauser = pauser.get();
        try {
            while (!lock.compareAndSwapValue(UNLOCKED, lockValueFromTid)) {
                if (count++ > 1000 && Thread.currentThread().isInterrupted())
                    throw new InterruptedRuntimeException("Interrupted");
                tlPauser.pause(timeout, TimeUnit.MILLISECONDS);
                value = lock.getVolatileValue();
            }
        } catch (TimeoutException e) {
            warnAndForceUnlock("Couldn't acquire lock", value);
            acquireLock();
        } finally {
            tlPauser.reset();
        }
    }

    private long getLockValueFromTid(long tid) {
        int tidi = Maths.toInt32(tid);
        if (tidi != tid)
            Jvm.error().on(getClass(), "Lossy conversion of threadid " + Long.toHexString(tid) + " to " + tidi);
        return tid << 32 | PID;
    }

    /**
     * checks if current thread holds lock. If not, it will wait for <code>chronicle.queue.lock.timeoutMS</code> millis for the lock to be
     * released, and if it is not after timeout, forcibly unlocks and continues.
     */
    // TODO combine logic for acquireLock with this method so recovery is consistent.
    @Override
    public void waitForLock() {
        throwExceptionIfClosed();

        long tid = Thread.currentThread().getId();
        if (isLockHeldByCurrentThread(tid))
            return;

        long value = lock.getVolatileValue();
        Pauser tlPauser = pauser.get();
        try {
            while (value != UNLOCKED) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedRuntimeException("Interrupted");
                tlPauser.pause(timeout, TimeUnit.MILLISECONDS);
                value = lock.getVolatileValue();
            }
        } catch (TimeoutException e) {
            warnAndForceUnlock("Queue lock is still held", value);
            // try again.
            waitForLock();

        } catch (NullPointerException ex) {
            if (!tableStore.isClosed())
                throw ex;
            throw new IllegalStateException("The table store is closed!", ex);
        } finally {
            tlPauser.reset();
        }
    }

    private void warnAndForceUnlock(String msg, long value) {
        warnLock(msg, value);
        if (forceUnlockOnTimeoutWhen == UnlockMode.LOCKING_PROCESS_DEAD) {
            if (!forceUnlockIfProcessIsDead())
                throw new UnrecoverableTimeoutException(new IllegalStateException(msg + UNLOCK_MAIN_MSG));
        } else {
            forceUnlock(value);
        }
    }

    private void warnLock(String msg, long value) {
        String pid = ((int) value == PID) ? "me" : Integer.toString((int) value);
        final String warningMsg = msg + " after " + timeout + "ms for " +
                "the lock file:" + path + ". Lock is held by " +
                "PID: " + pid + ", " +
                "TID: " + (int) (value >>> 32);
        if (forceUnlockOnTimeoutWhen == UnlockMode.NEVER)
            throw new UnrecoverableTimeoutException(new IllegalStateException(warningMsg + UNLOCK_MAIN_MSG));
        warn().on(getClass(), warningMsg + UNLOCKING_FORCIBLY_MSG);
    }

    /**
     * Checks if the lock is held by current thread and if so, releases it, removing entry from TableStore and clearing ThreadLocal state, allowing
     * anyone to proceed with {@link net.openhft.chronicle.queue.ChronicleQueue#acquireAppender}.  If it is already unlocked, no action is taken.
     */
    @Override
    public void unlock() {
        throwExceptionIfClosed();
        final long tid = Thread.currentThread().getId();
        if (!lock.compareAndSwapValue(getLockValueFromTid(tid), UNLOCKED)) {
            warn().on(getClass(), "Queue lock was locked by another thread, currentID=" + tid + ", lock-tid=" + lock.getVolatileValue() + " so this lock was not removed.");
        }
    }

    /**
     * unlike {@link TSQueueLock#unlock()} this method will not WARN if already unlocked
     */
    public void quietUnlock() {
        throwExceptionIfClosed();

        if (lockedBy() != UNLOCKED) {
            final long tid = Thread.currentThread().getId();
            if (!lock.compareAndSwapValue(getLockValueFromTid(tid), UNLOCKED)) {
                final long value = lock.getVolatileValue();
                if (value == UNLOCKED)
                    return;
                warn().on(getClass(), "Queue lock was locked by another thread, current-thread-tid=" + tid + ", lock value=" + value + ", this lock was not removed.");
            }
        }
    }

    @Override
    public boolean isLocked() {
        return lockedBy() != UNLOCKED;
    }

    /**
     * Force unlock if the lock is held by the specified thread ID
     */
    public void forceUnlockOnBehalfOfThread(long threadId) {
        throwExceptionIfClosed();
        if (isLocked() && !lock.compareAndSwapValue(getLockValueFromTid(threadId), UNLOCKED)) {
            warn().on(getClass(), "Queue lock was locked by another thread, provided-tid=" + threadId + ", lock-tid=" + lock.getVolatileValue() + " so this lock was not removed.");
        }
    }

    private boolean isLockHeldByCurrentThread(long tid) {
        return lock.getVolatileValue() == getLockValueFromTid(tid);
    }
}
