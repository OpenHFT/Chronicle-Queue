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
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.queue.impl.table.SingleTableBuilder;
import net.openhft.chronicle.threads.Pauser;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class TSQueueLock implements QueueLock {

    private static final String LOCK_KEY = "chronicle.queue.lock";
    private static final long LOCK_WAIT_TIMEOUT = Long.getLong("chronicle.queue.lock.timeoutMS", 30_000);
    private static final long UNLOCKED = 0L;
    private final LongValue lock;
    private final Pauser pauser;
    private final ThreadLocal<Long> lockHolderTidTL = new ThreadLocal<>();

    public TSQueueLock(File queueDirectoryPath, Supplier<Pauser> pauser) {
        final File storeFilePath;
        if ("".equals(queueDirectoryPath.getPath())) {
            storeFilePath = new File(DirectoryListing.DIRECTORY_LISTING_FILE);
        } else {
            storeFilePath = new File(queueDirectoryPath, DirectoryListing.DIRECTORY_LISTING_FILE);
            queueDirectoryPath.mkdirs();
        }
        TableStore tableStore = SingleTableBuilder.binary(storeFilePath).build();
        this.lock = tableStore.acquireValueFor(LOCK_KEY);
        this.pauser = pauser.get();
    }

    @Override
    public void checkLock() {
        if (isLockHeldByCurrentThread())
            return;

        try {
            while (lock.getVolatileValue() != UNLOCKED) {
                pauser.pause(LOCK_WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
            }
        } catch (TimeoutException e) {
            throw new IllegalStateException("Queue lock is still held after " + LOCK_WAIT_TIMEOUT + "ms. Unlock manually");
        } finally {
            pauser.reset();
        }
    }

    @Override
    public void acquireLock() {
        long tid = Thread.currentThread().getId();
        try {
            while (!lock.compareAndSwapValue(UNLOCKED, tid)) {
                pauser.pause(LOCK_WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
            }
        } catch (TimeoutException e) {
            throw new IllegalStateException("Couldn't acquire lock after " + LOCK_WAIT_TIMEOUT + "ms.");
        } finally {
            pauser.reset();
        }
    }

    @Override
    public void unlock() {
        if (!isLockHeldByCurrentThread())
            throw new IllegalStateException("Can't unlock when lock is not held by this thread");

        if (!lock.compareAndSwapValue(lockHolderTidTL.get(), UNLOCKED)) {
            Jvm.warn().on(getClass(), "Queue lock was unlocked by someone else!");
        }
        lockHolderTidTL.remove();
    }

    private boolean isLockHeldByCurrentThread() {
        long tid = Thread.currentThread().getId();
        Long lockHolderTid = lockHolderTidTL.get();
        return lockHolderTid != null && lockHolderTid == tid;
    }
}
