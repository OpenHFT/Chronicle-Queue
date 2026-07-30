/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.wire.MarshallableOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Immutable builder-side description of the advanced context-listener feature.
 *
 * <p>{@link SingleChronicleQueueBuilder} keeps this as its only context-listener field. A direct
 * listener is reused by every queue built from that builder; a supplier creates a separately owned
 * listener for each queue. Listener-free builders share {@link #NONE}, so the builder field remains
 * non-null without allocating configuration state per builder.</p>
 */
final class ContextListenerConfiguration {
    static final ContextListenerConfiguration NONE =
            new ContextListenerConfiguration(null, null, null);

    @Nullable
    private final Class<?> writerType;
    @Nullable
    private final MarshallableOut.ContextListener<?> listener;
    @Nullable
    private final Supplier<? extends MarshallableOut.ContextListener<?>> listenerSupplier;

    private ContextListenerConfiguration(@Nullable Class<?> writerType,
                                         @Nullable MarshallableOut.ContextListener<?> listener,
                                         @Nullable Supplier<? extends MarshallableOut.ContextListener<?>> listenerSupplier) {
        this.writerType = writerType;
        this.listener = listener;
        this.listenerSupplier = listenerSupplier;
    }

    static <T> ContextListenerConfiguration of(Class<T> writerType,
                                               MarshallableOut.ContextListener<? super T> listener) {
        return new ContextListenerConfiguration(requireNonNull(writerType), requireNonNull(listener), null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static <T> ContextListenerConfiguration supplied(Class<T> writerType,
                                                     Supplier<? extends MarshallableOut.ContextListener<? super T>> listenerSupplier) {
        return new ContextListenerConfiguration(
                requireNonNull(writerType), null, (Supplier) requireNonNull(listenerSupplier));
    }

    boolean configured() {
        return writerType != null;
    }

    @Nullable
    MarshallableOut.ContextListener<?> listener() {
        return listener;
    }

    @NotNull
    ContextListenerBinding resolve() {
        MarshallableOut.ContextListener<?> resolvedListener = newListener();
        return ContextListenerBinding.of(requireNonNull(writerType), resolvedListener);
    }

    @NotNull
    private MarshallableOut.ContextListener<?> newListener() {
        if (!configured())
            throw new IllegalStateException("No context listener is configured");
        return listenerSupplier == null
                ? requireNonNull(listener)
                : requireNonNull(listenerSupplier.get(), "contextListenerSupplier.get()");
    }
}
