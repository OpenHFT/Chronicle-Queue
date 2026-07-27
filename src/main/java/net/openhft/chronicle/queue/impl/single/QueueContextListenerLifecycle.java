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
 * Shared per-queue coordination and ownership for configured context listeners.
 *
 * <p>{@link SingleChronicleQueue} stores exactly one reference of this type. Listener-free queues
 * use {@link #NO_OP}; the active implementation is allocated only for a builder listener or when
 * an appender-local listener is first configured. Active state coordinates the at-most-once
 * callback across all appenders and reference-counts appender-owned listeners.</p>
 */
interface QueueContextListenerLifecycle extends AutoCloseable {
    /** Shared lifecycle for a queue with no listener state. */
    QueueContextListenerLifecycle NO_OP = NoOpQueueContextListenerLifecycle.INSTANCE;

    @Nullable
    Class<?> writerType();

    @Nullable
    MarshallableOut.ContextListener<?> listener();

    /**
     * Checks, under the queue write lock, whether the target cycle is still empty.
     *
     * @return {@code true} when the caller may write that cycle's context records
     */
    boolean shouldNotify(int cycle, @NotNull SingleChronicleQueueStore store, @NotNull ExcerptContext context);

    /** Records successful handling of a cycle, including a callback that wrote no documents. */
    void complete(int cycle);

    /** Acquires queue-local ownership of an appender listener. */
    void retain(@Nullable MarshallableOut.ContextListener<?> listener);

    /** Releases and, when appropriate, closes an appender listener. */
    void release(@Nullable MarshallableOut.ContextListener<?> listener);

    @Override
    void close();

    static QueueContextListenerLifecycle from(@Nullable ContextListenerConfiguration configuration) {
        return configuration == null
                ? NO_OP
                : new ActiveQueueContextListenerLifecycle(configuration.writerType(), configuration.newListener());
    }

    static QueueContextListenerLifecycle activeWithoutQueueListener() {
        return new ActiveQueueContextListenerLifecycle(null, null);
    }
}

/** Shared, allocation-free queue lifecycle used when no context listener has been configured. */
enum NoOpQueueContextListenerLifecycle implements QueueContextListenerLifecycle {
    INSTANCE;

    @Override
    public Class<?> writerType() {
        return null;
    }

    @Override
    public MarshallableOut.ContextListener<?> listener() {
        return null;
    }

    @Override
    public boolean shouldNotify(int cycle, @NotNull SingleChronicleQueueStore store, @NotNull ExcerptContext context) {
        return false;
    }

    @Override
    public void complete(int cycle) {
    }

    @Override
    public void retain(@Nullable MarshallableOut.ContextListener<?> listener) {
    }

    @Override
    public void release(@Nullable MarshallableOut.ContextListener<?> listener) {
    }

    @Override
    public void close() {
    }
}

/**
 * Active coordinator allocated only when a builder or appender listener is configured.
 *
 * <p>The volatile cycle cache is merely an in-process fast path. The authoritative empty-cycle
 * check is still performed while the cross-process write lock is held, so appenders in different
 * processes cannot both inject context records into the same cycle.</p>
 */
final class ActiveQueueContextListenerLifecycle implements QueueContextListenerLifecycle {
    @Nullable
    private final Class<?> writerType;
    @Nullable
    private final MarshallableOut.ContextListener<?> listener;
    private volatile long lastCycleChecked = Long.MIN_VALUE;
    @Nullable
    private Map<MarshallableOut.ContextListener<?>, int[]> appenderListenerRefs;

    ActiveQueueContextListenerLifecycle(@Nullable Class<?> writerType,
                                        @Nullable MarshallableOut.ContextListener<?> listener) {
        this.writerType = writerType;
        this.listener = listener;
    }

    @Nullable
    @Override
    public Class<?> writerType() {
        return writerType;
    }

    @Nullable
    @Override
    public MarshallableOut.ContextListener<?> listener() {
        return listener;
    }

    @Override
    public boolean shouldNotify(int cycle, @NotNull SingleChronicleQueueStore store, @NotNull ExcerptContext context) {
        if (lastCycleChecked >= cycle)
            return false;
        try {
            if (store.lastSequenceNumber(context) >= 0) {
                lastCycleChecked = cycle;
                return false;
            }
            return true;
        } catch (StreamCorruptedException e) {
            Jvm.warn().on(SingleChronicleQueue.class,
                    "Could not read last sequence for cycle " + cycle + "; skipping context listener", e);
            lastCycleChecked = cycle;
            return false;
        }
    }

    @Override
    public void complete(int cycle) {
        lastCycleChecked = Math.max(lastCycleChecked, cycle);
    }

    @Override
    public synchronized void retain(@Nullable MarshallableOut.ContextListener<?> listener) {
        if (listener == null || listener == this.listener)
            return;
        if (appenderListenerRefs == null)
            appenderListenerRefs = new IdentityHashMap<>();
        appenderListenerRefs.computeIfAbsent(listener, ignored -> new int[1])[0]++;
    }

    @Override
    public void release(@Nullable MarshallableOut.ContextListener<?> listener) {
        if (listener == null || listener == this.listener)
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

    @Override
    public void close() {
        Closeable.closeQuietly(listener);
    }
}
