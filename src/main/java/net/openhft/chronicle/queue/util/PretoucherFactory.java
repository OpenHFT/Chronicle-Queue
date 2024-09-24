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

package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.jetbrains.annotations.NotNull;

/**
 * Factory interface for creating {@link Pretoucher} and {@link EventHandler} instances.
 * <p>
 * Pretouchers are used to preload and access resources in advance, reducing the likelihood of delays due to I/O or
 * memory access. This interface provides methods to create both event handlers and pretouchers for a specific
 * {@link SingleChronicleQueue}.
 */
public interface PretoucherFactory {

    /**
     * Creates an {@link EventHandler} for the specified {@link SingleChronicleQueue}.
     * <p>The event handler can be used to periodically pretouch or handle other events related to the queue.
     *
     * @param queue The {@link SingleChronicleQueue} instance for which the event handler is created
     * @return A new {@link EventHandler} for the given queue
     */
    EventHandler createEventHandler(@NotNull final SingleChronicleQueue queue);

    /**
     * Creates a {@link Pretoucher} for the specified {@link SingleChronicleQueue}.
     * <p>The pretoucher is used to access and load queue resources in advance, reducing potential I/O-related delays
     * during critical operations.
     *
     * @param queue The {@link SingleChronicleQueue} instance for which the pretoucher is created
     * @return A new {@link Pretoucher} for the given queue
     */
    Pretoucher createPretoucher(@NotNull final SingleChronicleQueue queue);
}
