/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.wire.MarshallableOut;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Immutable builder-side description of the advanced context-listener feature.
 *
 * <p>{@link SingleChronicleQueueBuilder} keeps this as its only context-listener field. A direct
 * listener is reused by every queue built from that builder; a supplier creates a separately owned
 * listener for each queue. No configuration object exists for the normal, listener-free case.</p>
 */
final class ContextListenerConfiguration {
    private final Class<?> writerType;
    @Nullable
    private final MarshallableOut.ContextListener<?> listener;
    @Nullable
    private final Supplier<? extends MarshallableOut.ContextListener<?>> listenerSupplier;

    private ContextListenerConfiguration(Class<?> writerType,
                                         @Nullable MarshallableOut.ContextListener<?> listener,
                                         @Nullable Supplier<? extends MarshallableOut.ContextListener<?>> listenerSupplier) {
        this.writerType = requireNonNull(writerType);
        this.listener = listener;
        this.listenerSupplier = listenerSupplier;
    }

    static <T> ContextListenerConfiguration of(Class<T> writerType,
                                               MarshallableOut.ContextListener<? super T> listener) {
        return new ContextListenerConfiguration(writerType, requireNonNull(listener), null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static <T> ContextListenerConfiguration supplied(Class<T> writerType,
                                                     Supplier<? extends MarshallableOut.ContextListener<? super T>> listenerSupplier) {
        return new ContextListenerConfiguration(writerType, null, (Supplier) requireNonNull(listenerSupplier));
    }

    Class<?> writerType() {
        return writerType;
    }

    @Nullable
    MarshallableOut.ContextListener<?> listener() {
        return listener;
    }

    MarshallableOut.ContextListener<?> newListener() {
        return listenerSupplier == null
                ? requireNonNull(listener)
                : requireNonNull(listenerSupplier.get(), "contextListenerSupplier.get()");
    }
}
