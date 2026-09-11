/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Utility class for creating {@link Pretoucher} and {@link EventHandler} instances.
 * <p>
 * This class provides a factory-based mechanism to support both enterprise and non-enterprise pretoucher functionality.
 * If enterprise features are available, it attempts to load the enterprise implementation, otherwise, it falls back
 * to a basic implementation.
 */
public final class PretouchUtil {
    private static final PretoucherFactory INSTANCE;

    static {
        PretoucherFactory instance;
        try {
            final Class<?> clazz = Class.forName("software.chronicle.enterprise.queue.pretoucher.EnterprisePretouchUtil");
            instance = (PretoucherFactory) ObjectUtils.newInstance(clazz);
            assert SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable();
        } catch (Exception e) {
            instance = new PretouchFactoryEmpty();
            SingleChronicleQueueBuilder.onlyAvailableInEnterprise("Pretoucher");
        }
        INSTANCE = instance;
    }

    /**
     * Creates an {@link EventHandler} for the given {@link ChronicleQueue}.
     *
     * @param queue The {@link ChronicleQueue} instance
     * @return A new {@link EventHandler} for the specified queue
     */
    public static EventHandler createEventHandler(@NotNull final ChronicleQueue queue) {
        return INSTANCE.createEventHandler((SingleChronicleQueue) queue);
    }

    /**
     * Creates a {@link Pretoucher} for the given {@link ChronicleQueue}.
     *
     * @param queue The {@link ChronicleQueue} instance
     * @return A new {@link Pretoucher} for the specified queue
     */
    public static Pretoucher createPretoucher(@NotNull final ChronicleQueue queue) {
        return INSTANCE.createPretoucher((SingleChronicleQueue) queue);
    }

    /**
     * Fallback factory class for non-enterprise environments.
     * <p>
     * Provides basic implementations for creating {@link EventHandler} and {@link Pretoucher} when enterprise features
     * are not available.
     */
    private static class PretouchFactoryEmpty implements PretoucherFactory {

        /**
         * Returns a no-op {@link EventHandler} that does nothing.
         *
         * @param queue The {@link SingleChronicleQueue} instance
         * @return A no-op {@link EventHandler}
         */
        @Override
        public EventHandler createEventHandler(@NotNull final SingleChronicleQueue queue) {
            return () -> false;
        }

        /**
         * Creates a basic {@link Pretoucher} for the specified {@link SingleChronicleQueue}.
         *
         * @param queue The {@link SingleChronicleQueue} instance
         * @return A basic {@link Pretoucher} instance
         */
        @Override
        public Pretoucher createPretoucher(@NotNull final SingleChronicleQueue queue) {
            return queue.createPretoucher();
        }
    }
}
