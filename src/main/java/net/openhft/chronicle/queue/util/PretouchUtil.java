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

    // Singleton instance of the PretoucherFactory
    private static final PretoucherFactory INSTANCE;

    static {
        PretoucherFactory instance;
        try {
            // Attempt to load enterprise features
            final Class<?> clazz = Class.forName("software.chronicle.enterprise.queue.pretoucher.EnterprisePretouchUtil");
            instance = (PretoucherFactory) ObjectUtils.newInstance(clazz);
            assert SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable();
        } catch (Exception e) {
            // Fallback to the non-enterprise implementation
            instance = new PretouchFactoryEmpty();
            SingleChronicleQueueBuilder.onlyAvailableInEnterprise("Pretoucher");
        }
        INSTANCE = instance; // Set the determined instance
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
            return () -> false; // No-op event handler
        }

        /**
         * Creates a basic {@link Pretoucher} for the specified {@link SingleChronicleQueue}.
         *
         * @param queue The {@link SingleChronicleQueue} instance
         * @return A basic {@link Pretoucher} instance
         */
        @Override
        public Pretoucher createPretoucher(@NotNull final SingleChronicleQueue queue) {
            return queue.createPretoucher(); // Create a simple pretoucher
        }
    }
}
