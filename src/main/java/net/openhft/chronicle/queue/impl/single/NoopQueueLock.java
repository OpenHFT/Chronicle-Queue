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

/**
 * @deprecated To be removed in .22
 */
@Deprecated
public class NoopQueueLock implements QueueLock {

    @Override
    public void waitForLock() {
    }

    @Override
    public void acquireLock() {
    }

    @Override
    public void unlock() {
    }

    @Override
    public void quietUnlock() {
    }

    @Override
    public boolean isLocked() {
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
