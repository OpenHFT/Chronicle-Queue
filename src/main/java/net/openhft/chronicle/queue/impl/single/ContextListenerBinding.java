/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.wire.MarshallableOut;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNull;

/**
 * Resolved, non-null pairing of a context-listener method-writer type and callback.
 *
 * <p>The builder keeps the possibly supplier-backed {@link ContextListenerConfiguration}; a queue
 * resolves that description once into this value. The same value can then be shared with every
 * appender that inherits the queue listener. Appender-local configuration creates its own value.
 * Keeping the pair together prevents partially configured lifecycle state and confines the raw
 * callback invocation to one place.</p>
 */
final class ContextListenerBinding {
    private final Class<?> writerType;
    private final MarshallableOut.ContextListener<?> listener;

    private ContextListenerBinding(@NotNull Class<?> writerType,
                                   @NotNull MarshallableOut.ContextListener<?> listener) {
        this.writerType = requireNonNull(writerType);
        this.listener = requireNonNull(listener);
    }

    @NotNull
    static ContextListenerBinding of(@NotNull Class<?> writerType,
                                     @NotNull MarshallableOut.ContextListener<?> listener) {
        return new ContextListenerBinding(writerType, listener);
    }

    @NotNull
    MarshallableOut.ContextListener<?> listener() {
        return listener;
    }

    @NotNull
    Object newMethodWriter(@NotNull LockedContextListenerMarshallableOut out) {
        return out.methodWriter(writerType);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    void notifyListener(@NotNull Object methodWriter) {
        ((MarshallableOut.ContextListener) listener).onNewContext(methodWriter);
    }
}
