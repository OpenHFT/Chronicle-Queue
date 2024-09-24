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

import java.util.function.LongConsumer;

/**
 * A {@code ReadOnlyWriteLock} is a {@link WriteLock} implementation for read-only Chronicle Queues.
 * This lock throws {@link IllegalStateException} when any write-related operation is attempted,
 * indicating that the queue is in a read-only state.
 */
public class ReadOnlyWriteLock implements WriteLock {

    /**
     * Attempts to acquire the write lock.
     * This method always throws an {@link IllegalStateException} since the queue is read-only.
     *
     * @throws IllegalStateException if called, because the queue is read-only.
     */
    @Override
    public void lock() {
        throw new IllegalStateException("Queue is read-only");
    }

    /**
     * Attempts to release the write lock.
     * This method always throws an {@link IllegalStateException} since the queue is read-only.
     *
     * @throws IllegalStateException if called, because the queue is read-only.
     */
    @Override
    public void unlock() {
        throw new IllegalStateException("Queue is read-only");
    }

    /**
     * Closes the write lock.
     * This method does nothing for read-only locks.
     */
    @Override
    public void close() {
        // No operation needed for a read-only lock.
    }

    /**
     * Attempts to forcefully unlock if the locking process is dead.
     * This method always throws an {@link IllegalStateException} since the queue is read-only.
     *
     * @throws IllegalStateException if called, because the queue is read-only.
     */
    @Override
    public boolean forceUnlockIfProcessIsDead() {
        throw new IllegalStateException("Queue is read-only");
    }

    /**
     * Checks if the lock is held by the current process.
     * Since the queue is read-only, this method always returns false and triggers the provided consumer
     * with {@link Long#MAX_VALUE}, which represents an invalid process.
     *
     * @param notCurrentProcessConsumer the consumer that will be called with {@link Long#MAX_VALUE}.
     * @return false, as the lock is not held by the current process (read-only queue).
     */
    @Override
    public boolean isLockedByCurrentProcess(LongConsumer notCurrentProcessConsumer) {
        notCurrentProcessConsumer.accept(Long.MAX_VALUE);
        return false;
    }
}
