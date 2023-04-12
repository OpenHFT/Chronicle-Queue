/*
 * Copyright 2016-2022 chronicle.software
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

import org.jetbrains.annotations.NotNull;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * An adapter so we can use the QueueLock as an acquireAppenderCondition for backward
 * compatibility
 */
public class QueueLockUnlockedCondition implements Condition {

    private final SingleChronicleQueue singleChronicleQueue;

    public QueueLockUnlockedCondition(SingleChronicleQueue singleChronicleQueue) {
        this.singleChronicleQueue = singleChronicleQueue;
    }

    @Override
    public void await() throws InterruptedException {
        singleChronicleQueue.queueLock().waitForLock();
    }

    @Override
    public void awaitUninterruptibly() {
        singleChronicleQueue.queueLock().waitForLock();
    }

    @Override
    public long awaitNanos(long l) {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public boolean await(long l, TimeUnit timeUnit) throws InterruptedException {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public boolean awaitUntil(@NotNull Date date) {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void signal() {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void signalAll() {
        throw new UnsupportedOperationException("unsupported");
    }
}
