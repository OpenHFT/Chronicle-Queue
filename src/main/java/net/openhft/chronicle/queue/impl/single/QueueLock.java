/*
 * Copyright 2016-2022 Chronicle Software
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

import net.openhft.chronicle.core.io.Closeable;

/**
 * @deprecated use {@link WriteLock} instead.
 */
@Deprecated(/* To be removed in x.26 */)
public interface QueueLock extends Closeable {

    /**
     * For internal use only
     *
     * @deprecated
     */
    @Deprecated(/* To be removed in x.26 */)
    void waitForLock();

    /**
     * Guaranteed to lock or throw
     *
     * @deprecated use {@link WriteLock#locked()} instead.
     */
    @Deprecated(/* To be removed in x.26 */)
    void acquireLock();

    /**
     * Tries to unlock, and if it can't, logs a warning
     */
    void unlock();

    /**
     * only unlocks if locked
     */
    void quietUnlock();

    /**
     * Is this lock locked?
     * @deprecated use {@link WriteLock#locked()} instead.
     *
     * @return true if the lock is locked
     */
    @Deprecated(/* To be removed in x.26 */)
    boolean isLocked();
}
