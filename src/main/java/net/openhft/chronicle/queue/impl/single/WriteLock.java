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

package net.openhft.chronicle.queue.impl.single;

import java.io.Closeable;
import java.util.function.LongConsumer;

/**
 * The WriteLock interface provides methods to control locking mechanisms in a Chronicle Queue.
 * It defines locking, unlocking, and checking mechanisms, ensuring exclusive access to resources
 * while preventing race conditions.
 *
 * This interface is non-reentrant, meaning that once a lock is acquired, it cannot be reacquired by
 * the same process until it is explicitly released.
 */
public interface WriteLock extends Closeable {

    /**
     * A no-operation implementation of the WriteLock interface. This version of WriteLock performs no actions
     * when methods are invoked, making it suitable as a default or placeholder.
     */
    WriteLock NO_OP = new WriteLock() {

        /**
         * No-op implementation for lock.
         */
        @Override
        public void lock() {
        }

        /**
         * No-op implementation for unlock.
         */
        @Override
        public void unlock() {
        }

        /**
         * No-op implementation for close.
         */
        @Override
        public void close() {
        }

        /**
         * Simulates a successful unlock for a dead process.
         *
         * @return always returns {@code true} indicating that the lock is unlocked.
         */
        @Override
        public boolean forceUnlockIfProcessIsDead() {
            return true;
        }

        /**
         * Simulates that the current process holds the lock.
         *
         * @param notCurrentProcessConsumer a consumer for processes that do not hold the lock.
         * @return always returns {@code true}.
         */
        @Override
        public boolean isLockedByCurrentProcess(LongConsumer notCurrentProcessConsumer) {
            return true;
        }
    };

    /**
     * Acquires the lock, ensuring exclusive access to the resource.
     * <p>
     * This method is guaranteed to succeed in acquiring the lock, even if it involves waiting for a timeout or recovering
     * from a previously held lock. The lock is not reentrant, meaning it cannot be reacquired by the same thread until it
     * is released.
     */
    void lock();

    /**
     * Releases the lock, allowing other threads or processes to acquire it.
     * <p>
     * This method may fail to unlock, in which case a warning will be logged.
     */
    void unlock();

    /**
     * Closes the lock resource. This method is invoked when the lock is no longer needed.
     */
    void close();

    /**
     * Checks if the lock is currently held by any process.
     *
     * @return {@code false} by default, as this method is designed to be overridden by implementations that support checking.
     */
    default boolean locked() {
        return false;
    }

    /**
     * Forcibly unlocks only if the process that currently holds the lock is no-longer running.
     * <p>
     * This will leave the lock in following states:
     * <ul>
     *   <li>
     *     <b>unlocked</b>
     *     <ul>
     *       <li>if the lock was already unlocked</li>
     *       <li>if the lock was held by a dead process</li>
     *     </ul>
     *   </li>
     *   <li>
     *     <b>locked</b>
     *     <ul>
     *       <li>if the lock was held by the current process</li>
     *       <li>if the lock was held by another live process</li>
     *     </ul>
     *   </li>
     * </ul>
     *
     * @return {@code true} lock was left in an <b>unlocked</b> state, {@code false} if the lock was left in a <b>locked</b> state.
     */
    boolean forceUnlockIfProcessIsDead();

    /**
     * Checks if the current process holds the lock and provides a callback if not.
     * <p>
     * This method verifies if the current process holds the lock. If the lock is held by another process,
     * the provided {@link LongConsumer} is invoked with the ID of the process holding the lock.
     *
     * @param notCurrentProcessConsumer a {@link LongConsumer} that is invoked when the lock is held by another process.
     * @return {@code true} if the current process holds the lock, otherwise {@code false}.
     */
    boolean isLockedByCurrentProcess(LongConsumer notCurrentProcessConsumer);
}
