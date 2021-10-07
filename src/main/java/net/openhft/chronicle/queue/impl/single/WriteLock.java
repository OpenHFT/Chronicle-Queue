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

import java.io.Closeable;
import java.util.function.LongConsumer;

public interface WriteLock extends Closeable {

    WriteLock NO_OP = new WriteLock() {

        @Override
        public void lock() {
        }

        @Override
        public void unlock() {
        }

        @Override
        public void close() {
        }

        @Override
        public boolean forceUnlockIfProcessIsDead() {
            return true;
        }

        @Override
        public boolean isLockedByCurrentProcess(LongConsumer notCurrentProcessConsumer) {
            return true;
        }
    };

    /**
     * Guaranteed to succeed in getting the lock (may involve timeout and recovery) or else throw.
     * <p>This is not re-entrant i.e. if you lock and try and lock again it will timeout and recover
     */
    void lock();

    /**
     * May not unlock. If it does not there will be a log.warn
     */
    void unlock();

    void close();

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

    boolean isLockedByCurrentProcess(LongConsumer notCurrentProcessConsumer);
}
