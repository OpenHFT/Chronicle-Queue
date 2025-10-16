/*
 * Copyright 2016-2025 chronicle.software
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
 * A condition that is always true
 */
public final class NoOpCondition implements Condition {

    public static final NoOpCondition INSTANCE = new NoOpCondition();

    private NoOpCondition() {}

    @Override
    public void await() throws InterruptedException {
    }

    @Override
    public void awaitUninterruptibly() {
    }

    @Override
    public long awaitNanos(long l) {
        return l;
    }

    @Override
    public boolean await(long l, TimeUnit timeUnit) throws InterruptedException {
        return true;
    }

    @Override
    public boolean awaitUntil(@NotNull Date date) {
        return true;
    }

    @Override
    public void signal() {
    }

    @Override
    public void signalAll() {
    }
}
