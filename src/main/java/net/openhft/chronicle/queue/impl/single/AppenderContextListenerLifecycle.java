/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.wire.MarshallableOut;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;

/**
 * Per-appender entry points and state for the advanced context-listener feature.
 *
 * <p>{@link StoreAppender} stores exactly one reference of this type. Before an unconfigured
 * appender writes, that field is {@code null}, preserving the opportunity to configure an
 * appender-local listener. Its first write changes the field to {@link #NO_OP}; configured
 * appenders instead hold an {@link ActiveAppenderContextListenerLifecycle}. The hot write paths
 * compare against {@code NO_OP} before invoking these methods, so listener-free users do not make
 * an interface call.</p>
 */
interface AppenderContextListenerLifecycle extends AutoCloseable {
    /** Shared state for an appender that has started without a listener. */
    AppenderContextListenerLifecycle NO_OP = NoOpAppenderContextListenerLifecycle.INSTANCE;

    /** @return whether this appender has made its first write attempt */
    boolean started();

    /** Marks a write attempt and rejects callback re-entry through the public appender. */
    void onWriteAttempt();

    /**
     * Runs the callback, when required, before a regular document.
     *
     * @return {@code true} when listener output changed the appender position
     */
    boolean beforeDocument(boolean metaData, long safeLength);

    /**
     * Runs the callback, when required, before a raw document.
     *
     * @return {@code true} when listener output changed the appender position
     */
    boolean beforeRawDocument(long safeLength);

    /** Replaces the appender-local listener before the first write attempt. */
    AppenderContextListenerLifecycle configure(Class<?> writerType,
                                                MarshallableOut.ContextListener<?> listener);

    @Override
    void close();

    static AppenderContextListenerLifecycle active(StoreAppender appender,
                                                   StoreAppender.StoreAppenderContext context,
                                                   QueueContextListenerLifecycle queueLifecycle,
                                                   Class<?> writerType,
                                                   MarshallableOut.ContextListener<?> listener) {
        return new ActiveAppenderContextListenerLifecycle(
                appender, context, queueLifecycle, writerType, listener);
    }
}

/** Shared, allocation-free appender lifecycle used after the first listener-free write attempt. */
enum NoOpAppenderContextListenerLifecycle implements AppenderContextListenerLifecycle {
    INSTANCE;

    @Override
    public boolean started() {
        return true;
    }

    @Override
    public void onWriteAttempt() {
    }

    @Override
    public boolean beforeDocument(boolean metaData, long safeLength) {
        return false;
    }

    @Override
    public boolean beforeRawDocument(long safeLength) {
        return false;
    }

    @Override
    public AppenderContextListenerLifecycle configure(Class<?> writerType,
                                                       MarshallableOut.ContextListener<?> listener) {
        throw new IllegalStateException("Cannot change contextListener after this appender has written");
    }

    @Override
    public void close() {
    }
}

/**
 * Active appender lifecycle allocated only for a configured listener.
 *
 * <p>It owns appender-local listener references, prevents re-entry, and creates a fresh
 * {@link LockedContextListenerMarshallableOut} for each callback. The fresh callback adapter is
 * intentional: closing it invalidates any method writer retained beyond the write-lock scope.</p>
 */
final class ActiveAppenderContextListenerLifecycle implements AppenderContextListenerLifecycle {
    private final StoreAppender appender;
    private final StoreAppender.StoreAppenderContext context;
    private final QueueContextListenerLifecycle queueLifecycle;
    private Class<?> writerType;
    private MarshallableOut.ContextListener<?> listener;
    private boolean started;
    private boolean notifying;

    ActiveAppenderContextListenerLifecycle(StoreAppender appender,
                                           StoreAppender.StoreAppenderContext context,
                                           QueueContextListenerLifecycle queueLifecycle,
                                           Class<?> writerType,
                                           MarshallableOut.ContextListener<?> listener) {
        this.appender = appender;
        this.context = context;
        this.queueLifecycle = queueLifecycle;
        this.writerType = requireNonNull(writerType);
        this.listener = requireNonNull(listener);
        queueLifecycle.retain(listener);
    }

    @Override
    public boolean started() {
        return started;
    }

    @Override
    public void onWriteAttempt() {
        if (notifying)
            throw new IllegalStateException("Cannot write to the appender from " +
                    "within a ContextListener; write through the supplied method writer instead");
        started = true;
    }

    @Override
    public boolean beforeDocument(boolean metaData, long safeLength) {
        if (metaData || !appender.contextListenerWriteReady())
            return false;
        return notifyIfNeeded(safeLength);
    }

    @Override
    public boolean beforeRawDocument(long safeLength) {
        appender.resetPositionForContextListener();
        return notifyIfNeeded(safeLength);
    }

    private boolean notifyIfNeeded(long safeLength) {
        if (!queueLifecycle.shouldNotify(appender.cycle(), appender.store, appender))
            return false;

        notifying = true;
        try {
            notifyListener(safeLength);
            queueLifecycle.complete(appender.cycle());
            return true;
        } finally {
            notifying = false;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void notifyListener(long safeLength) {
        LockedContextListenerMarshallableOut out = new LockedContextListenerMarshallableOut(
                appender, context, appender.queue().wireType(), safeLength);
        try {
            ((MarshallableOut.ContextListener) listener).onNewContext(out.methodWriter(writerType));
        } finally {
            out.close();
        }
    }

    @Override
    public AppenderContextListenerLifecycle configure(@NotNull Class<?> writerType,
                                                       @NotNull MarshallableOut.ContextListener<?> listener) {
        if (started)
            throw new IllegalStateException("Cannot change contextListener after this appender has written");
        requireNonNull(writerType);
        requireNonNull(listener);

        MarshallableOut.ContextListener<?> previous = this.listener;
        if (previous != listener) {
            queueLifecycle.retain(listener);
            queueLifecycle.release(previous);
        }
        this.writerType = writerType;
        this.listener = listener;
        return this;
    }

    @Override
    public void close() {
        queueLifecycle.release(listener);
    }
}
