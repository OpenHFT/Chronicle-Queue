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
 * appenders instead hold an {@link ActiveContextListenerLifecycle}. The hot write paths compare
 * against {@code NO_OP} before invoking these methods, so listener-free users do not make an
 * interface call.</p>
 *
 * <p>Queue-wide cycle coordination and listener ownership are provided by the concrete
 * {@link ContextListenerCoordinator}; they are intentionally not mixed into this write-facing
 * interface.</p>
 */
interface ContextListenerLifecycle extends AutoCloseable {
    /** Shared state for an appender that has started without a listener. */
    ContextListenerLifecycle NO_OP = NoOpContextListenerLifecycle.INSTANCE;

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
    ContextListenerLifecycle configure(Class<?> writerType,
                                       MarshallableOut.ContextListener<?> listener);

    @Override
    void close();

    @NotNull
    static ContextListenerLifecycle active(@NotNull StoreAppender appender,
                                           @NotNull StoreAppender.StoreAppenderContext context,
                                           @NotNull ContextListenerCoordinator coordinator,
                                           @NotNull ContextListenerBinding binding) {
        return new ActiveContextListenerLifecycle(appender, context, coordinator, binding);
    }
}

/** Shared, allocation-free lifecycle used after the first listener-free write attempt. */
enum NoOpContextListenerLifecycle implements ContextListenerLifecycle {
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
    public ContextListenerLifecycle configure(Class<?> writerType,
                                              MarshallableOut.ContextListener<?> listener) {
        throw new IllegalStateException(
                "Cannot change contextListener after this appender has written");
    }

    @Override
    public void close() {
    }
}

/**
 * Active lifecycle allocated only for a configured appender.
 *
 * <p>It prevents re-entry and creates one method writer when the appender is configured. The
 * appender-bound output enables that writer only for the callback thread while the write lock is
 * held, avoiding proxy construction on every roll. The shared coordinator owns cycle-wide and
 * listener-reference state.</p>
 */
final class ActiveContextListenerLifecycle implements ContextListenerLifecycle {
    private final StoreAppender appender;
    private final ContextListenerCoordinator coordinator;
    private final LockedContextListenerMarshallableOut out;
    private ContextListenerBinding binding;
    private Object methodWriter;
    private boolean started;
    private boolean notifying;

    ActiveContextListenerLifecycle(@NotNull StoreAppender appender,
                                   @NotNull StoreAppender.StoreAppenderContext context,
                                   @NotNull ContextListenerCoordinator coordinator,
                                   @NotNull ContextListenerBinding binding) {
        this.appender = requireNonNull(appender);
        this.coordinator = requireNonNull(coordinator);
        this.binding = requireNonNull(binding);
        this.out = new LockedContextListenerMarshallableOut(
                appender, requireNonNull(context), appender.queue().wireType());
        this.methodWriter = binding.newMethodWriter(out);
        coordinator.retain(binding.listener());
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
        if (!coordinator.shouldNotify(appender.cycle(), appender.store, appender))
            return false;

        notifying = true;
        try {
            notifyListener(safeLength);
            coordinator.complete(appender.cycle());
            return true;
        } finally {
            notifying = false;
        }
    }

    private void notifyListener(long safeLength) {
        out.beginCallback(safeLength);
        try {
            binding.notifyListener(methodWriter);
        } finally {
            out.endCallback();
        }
    }

    @Override
    public ContextListenerLifecycle configure(@NotNull Class<?> writerType,
                                              @NotNull MarshallableOut.ContextListener<?> listener) {
        if (started)
            throw new IllegalStateException(
                    "Cannot change contextListener after this appender has written");

        ContextListenerBinding replacement = ContextListenerBinding.of(
                requireNonNull(writerType), requireNonNull(listener));
        Object replacementWriter = replacement.newMethodWriter(out);

        MarshallableOut.ContextListener<?> previous = binding.listener();
        if (previous != listener) {
            coordinator.retain(listener);
            coordinator.release(previous);
        }
        binding = replacement;
        methodWriter = replacementWriter;
        return this;
    }

    @Override
    public void close() {
        coordinator.release(binding.listener());
    }
}
