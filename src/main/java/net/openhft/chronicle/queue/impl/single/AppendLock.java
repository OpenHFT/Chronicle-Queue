/*
 * Copyright 2014-2024 chronicle.software
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

import net.openhft.chronicle.queue.impl.TableStore;
import net.openhft.chronicle.threads.TimingPauser;

import java.util.function.Supplier;

/**
 * The {@code AppendLock} class is a specialized implementation of {@link TableStoreWriteLock}.
 * It manages exclusive write access for appending data to a Chronicle Queue.
 * This class handles locking operations and includes mechanisms to forcefully unlock
 * if the process holding the lock is no longer running.
 */
public class AppendLock extends TableStoreWriteLock {

    /**
     * Constructs an {@code AppendLock} with the provided {@link TableStore}, {@link TimingPauser} supplier, and timeout.
     * The append lock ensures controlled access for appending operations.
     *
     * @param tableStore The {@link TableStore} associated with this lock.
     * @param pauser     The {@link TimingPauser} supplier used to manage lock acquisition retries.
     * @param timeoutMs  The maximum time, in milliseconds, to attempt acquiring the lock before timing out.
     */
    public AppendLock(TableStore<?> tableStore, Supplier<TimingPauser> pauser, Long timeoutMs) {
        super(tableStore, pauser, timeoutMs, TableStoreWriteLock.APPEND_LOCK_KEY);
    }

    /**
     * Attempts to forcefully unlock the lock only if the process that holds the lock is no longer running.
     * This is an override that disables warnings during force unlocking.
     *
     * @return {@code true} if the lock was successfully unlocked, {@code false} otherwise.
     */
    @Override
    public boolean forceUnlockIfProcessIsDead() {
        return super.forceUnlockIfProcessIsDead(false);
    }
}
