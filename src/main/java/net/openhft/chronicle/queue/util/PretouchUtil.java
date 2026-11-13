/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for creating {@link Pretoucher} and {@link EventHandler} instances.
 * <p>
 * This class provides a factory-based mechanism to support both enterprise and non-enterprise pretoucher functionality.
 * If enterprise features are available, it attempts to load the enterprise implementation, otherwise, it falls back
 * to a basic implementation whose {@link #createPretoucher(ChronicleQueue)} throws
 * {@link UnsupportedOperationException}.
 */
public final class PretouchUtil {
    static final String ENTERPRISE_PRETOUCH_UTIL = "software.chronicle.enterprise.queue.pretoucher.EnterprisePretouchUtil";
    private static final PretoucherFactory INSTANCE;

    static {
        final PretoucherFactory instance = loadFactory(ENTERPRISE_PRETOUCH_UTIL);
        if (instance instanceof PretouchFactoryEmpty) {
            if (((PretouchFactoryEmpty) instance).cause == null)
                SingleChronicleQueueBuilder.onlyAvailableInEnterprise("Pretoucher");
        } else {
            assert SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable();
        }
        INSTANCE = instance;
    }

    /**
     * Loads the named {@link PretoucherFactory}, falling back to a factory that reports why pretouching is unavailable.
     * An absent class means Chronicle Queue Enterprise is not on the classpath; any other failure means an
     * incompatible Enterprise extension was found, and is reported with its cause.
     *
     * @param className the fully qualified name of the factory to load
     * @return the loaded factory, or the open-source fallback
     */
    @NotNull
    static PretoucherFactory loadFactory(@NotNull final String className) {
        try {
            return (PretoucherFactory) ObjectUtils.newInstance(Class.forName(className));
        } catch (ClassNotFoundException absent) {
            return new PretouchFactoryEmpty(null);
        } catch (Exception | LinkageError incompatible) {
            Jvm.warn().on(PretouchUtil.class, "Pretoucher factory " + className + " is present but could not be loaded; pretouching is unavailable", incompatible);
            return new PretouchFactoryEmpty(incompatible);
        }
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
     * @throws UnsupportedOperationException if Chronicle Queue Enterprise is not available
     */
    public static Pretoucher createPretoucher(@NotNull final ChronicleQueue queue) {
        return INSTANCE.createPretoucher((SingleChronicleQueue) queue);
    }

    /**
     * Fallback factory class for non-enterprise environments.
     * <p>
     * Provides a no-op {@link EventHandler}; creating a {@link Pretoucher} is not supported.
     */
    static final class PretouchFactoryEmpty implements PretoucherFactory {
        @Nullable
        final Throwable cause;

        PretouchFactoryEmpty(@Nullable final Throwable cause) {
            this.cause = cause;
        }

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
         * Pretouching requires Chronicle Queue Enterprise.
         *
         * @param queue The {@link SingleChronicleQueue} instance
         * @throws UnsupportedOperationException always
         */
        @Override
        public Pretoucher createPretoucher(@NotNull final SingleChronicleQueue queue) {
            final String message = cause == null
                    ? "Pretoucher requires Chronicle Queue Enterprise, which is not on the classpath"
                    : "Pretoucher requires Chronicle Queue Enterprise; the Enterprise pretoucher on the classpath could not be loaded";
            throw new UnsupportedOperationException(message, cause);
        }
    }
}
