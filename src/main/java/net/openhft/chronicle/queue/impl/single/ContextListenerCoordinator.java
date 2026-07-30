/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.impl.ExcerptContext;
import net.openhft.chronicle.wire.MarshallableOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.StreamCorruptedException;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Queue-scoped coordination and ownership for the advanced context-listener feature.
 *
 * <p>This is deliberately a coordinator rather than another lifecycle interface. It provides the
 * state that must be shared by all appenders: the resolved queue-default listener, at-most-once
 * notification per roll cycle, and queue-local ownership of appender listeners. The actual write
 * entry points live in {@link ContextListenerLifecycle}.</p>
 *
 * <p>Listener-free queues share {@link #NONE}. An active coordinator is allocated only when a
 * builder listener exists or an appender-local listener is configured.</p>
 */
final class ContextListenerCoordinator implements AutoCloseable {
    static final ContextListenerCoordinator NONE = new ContextListenerCoordinator(null);

    @Nullable
    private final ContextListenerBinding defaultBinding;
    private volatile long lastCycleChecked = Long.MIN_VALUE;
    @Nullable
    private Map<MarshallableOut.ContextListener<?>, int[]> appenderListenerRefs;

    private ContextListenerCoordinator(@Nullable ContextListenerBinding defaultBinding) {
        this.defaultBinding = defaultBinding;
    }

    @NotNull
    static ContextListenerCoordinator from(@NotNull ContextListenerConfiguration configuration) {
        return configuration.configured()
                ? new ContextListenerCoordinator(configuration.resolve())
                : NONE;
    }

    @NotNull
    static ContextListenerCoordinator activeWithoutQueueListener() {
        return new ContextListenerCoordinator(null);
    }

    @Nullable
    ContextListenerBinding defaultBinding() {
        return defaultBinding;
    }

    /**
     * Checks, under the queue write lock, whether the target cycle is still empty.
     *
     * @return {@code true} when the caller may write that cycle's context records
     */
    boolean shouldNotify(int cycle,
                         @NotNull SingleChronicleQueueStore store,
                         @NotNull ExcerptContext context) {
        if (this == NONE || lastCycleChecked >= cycle)
            return false;
        try {
            if (store.lastSequenceNumber(context) >= 0) {
                lastCycleChecked = cycle;
                return false;
            }
            return true;
        } catch (StreamCorruptedException e) {
            Jvm.warn().on(SingleChronicleQueue.class,
                    "Could not read last sequence for cycle " + cycle +
                            "; skipping context listener", e);
            lastCycleChecked = cycle;
            return false;
        }
    }

    /** Records successful handling of a cycle, including a callback that wrote no documents. */
    void complete(int cycle) {
        if (this != NONE)
            lastCycleChecked = Math.max(lastCycleChecked, cycle);
    }

    /** Acquires queue-local ownership of an appender listener. */
    synchronized void retain(@NotNull MarshallableOut.ContextListener<?> listener) {
        if (this == NONE || isQueueListener(listener))
            return;
        if (appenderListenerRefs == null)
            appenderListenerRefs = new IdentityHashMap<>();
        appenderListenerRefs.computeIfAbsent(listener, ignored -> new int[1])[0]++;
    }

    /** Releases and, when appropriate, closes an appender listener. */
    void release(@NotNull MarshallableOut.ContextListener<?> listener) {
        if (this == NONE || isQueueListener(listener))
            return;

        boolean close = false;
        synchronized (this) {
            if (appenderListenerRefs == null)
                return;
            int[] count = appenderListenerRefs.get(listener);
            if (count == null)
                return;
            if (--count[0] <= 0) {
                appenderListenerRefs.remove(listener);
                close = true;
            }
        }
        if (close)
            Closeable.closeQuietly(listener);
    }

    private boolean isQueueListener(@NotNull MarshallableOut.ContextListener<?> listener) {
        return defaultBinding != null && listener == defaultBinding.listener();
    }

    @Override
    public void close() {
        if (defaultBinding != null)
            Closeable.closeQuietly(defaultBinding.listener());
    }
}
