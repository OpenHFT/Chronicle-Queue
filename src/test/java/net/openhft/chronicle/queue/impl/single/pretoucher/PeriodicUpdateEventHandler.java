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

package net.openhft.chronicle.queue.impl.single.pretoucher;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.threads.TimedEventHandler;

import java.util.function.LongConsumer;
import java.util.function.Supplier;

public final class PeriodicUpdateEventHandler extends TimedEventHandler {
    private final Supplier<LongConsumer> methodSupplier;
    private final long periodInMicroseconds;
    private final long startTimeMillis;
    private LongConsumer periodicUpdateHandler;

    PeriodicUpdateEventHandler(final Supplier<LongConsumer> methodSupplier,
                               long startTimeMillis, final long periodInMicroseconds) {
        this.methodSupplier = methodSupplier;
        this.startTimeMillis = startTimeMillis;
        this.periodInMicroseconds = periodInMicroseconds;
    }

    @Override
    protected long timedAction() throws InvalidEventHandlerException {
        long now = System.currentTimeMillis();
        if (now < startTimeMillis)
            return now - startTimeMillis;

        periodicUpdateHandler.accept(now);

        if (periodInMicroseconds == 0)
            throw new InvalidEventHandlerException("just runs once");

        // Note that this does not account for coordinated ommission - we should return actual time until we should run next
        return periodInMicroseconds;
    }

    @Override
    public void eventLoop(final EventLoop eventLoop) {
        // called on the event-loop thread, so at this point it is safe to acquire the appender
        periodicUpdateHandler = methodSupplier.get();
    }
}
