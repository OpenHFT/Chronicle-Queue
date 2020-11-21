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

import net.openhft.chronicle.core.io.Closeable;

public interface QueueLock extends Closeable {

    void waitForLock();

    /**
     * Guaranteed to lock or throw
     */
    void acquireLock();

    /**
     * Tries to unlock, and if it can't, logs a warning
     */
    void unlock();

    /**
     * only unlocks if locked
     */
    void quietUnlock();
}
