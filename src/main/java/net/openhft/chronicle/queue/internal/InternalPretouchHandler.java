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

package net.openhft.chronicle.queue.internal;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.HandlerPriority;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.jetbrains.annotations.NotNull;

public final class InternalPretouchHandler implements EventHandler {
    private final Pretoucher pretoucher;
    private long lastRun = 0;

    public InternalPretouchHandler(final SingleChronicleQueue queue) {
        this.pretoucher = new Pretoucher(queue);
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {
        long now = System.currentTimeMillis();
        // don't check too often.
        if (now > lastRun + 250) {
            pretoucher.execute();
            lastRun = now;
        }
        return false;
    }

    @NotNull
    @Override
    public HandlerPriority priority() {
        return HandlerPriority.MONITOR;
    }

    public void shutdown() {
        pretoucher.shutdown();
    }
}