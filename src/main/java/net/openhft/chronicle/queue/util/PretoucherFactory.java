/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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
