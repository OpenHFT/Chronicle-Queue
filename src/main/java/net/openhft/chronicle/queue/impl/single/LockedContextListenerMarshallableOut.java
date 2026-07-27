/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodWriterBuilder;
import net.openhft.chronicle.wire.BinaryMethodWriterInvocationHandler;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

/**
 * A callback-scoped {@link MarshallableOut} that writes through an already-locked
 * {@link StoreAppender}.
 *
 * <p>{@link StoreAppender} invokes a context listener while holding its non-reentrant write lock.
 * Using the appender's public write methods from that callback would attempt to acquire the same
 * lock again. This adapter instead opens documents directly on the locked appender and returns
 * {@link ContextListenerDocumentContext} instances that commit without unlocking it.</p>
 *
 * <p>The adapter is deliberately short-lived and not thread-safe. {@link #close()} is called when
 * the listener callback returns; it invalidates the adapter so a method writer retained by the
 * listener cannot write outside the locked callback.</p>
 */
final class LockedContextListenerMarshallableOut implements MarshallableOut {
    private final StoreAppender appender;
    private final StoreAppender.StoreAppenderContext context;
    private final WireType wireType;
    private final long safeLength;
    private ContextListenerDocumentContext activeContext;
    private boolean closed;

    /**
     * Creates an output for one context-listener callback.
     *
     * @param appender   appender whose write lock is already held
     * @param context    appender context used for the listener documents
     * @param wireType   wire type used to construct the listener's method writer
     * @param safeLength maximum number of bytes that may be written without overlapping a mapping
     */
    LockedContextListenerMarshallableOut(StoreAppender appender,
                                         StoreAppender.StoreAppenderContext context,
                                         WireType wireType,
                                         long safeLength) {
        this.appender = appender;
        this.context = context;
        this.wireType = wireType;
        this.safeLength = safeLength;
    }

    @NotNull
    @Override
    public DocumentContext writingDocument() {
        return writingDocument(false);
    }

    @Override
    public DocumentContext writingDocument(boolean metaData) {
        return acquireWritingDocument(metaData);
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) {
        if (closed)
            throw new IllegalStateException("ContextListener method writer cannot be used after the callback returns");
        if (context.wire() != null && context.isOpen() && context.chainedElement() && activeContext != null)
            return activeContext;

        appender.openContextForContextListener(metaData, safeLength);
        return activeContext = new ContextListenerDocumentContext(appender, context);
    }

    @NotNull
    @Override
    public <T> MethodWriterBuilder<T> methodWriterBuilder(boolean metaData, @NotNull Class<T> tClass) {
        VanillaMethodWriterBuilder<T> builder = new VanillaMethodWriterBuilder<>(tClass,
                wireType,
                () -> new BinaryMethodWriterInvocationHandler(tClass, metaData,
                        () -> LockedContextListenerMarshallableOut.this));
        builder.marshallableOut(this);
        builder.metaData(metaData);
        return builder;
    }

    @Override
    public void rollbackIfNotComplete() {
        context.rollbackIfNotComplete();
    }

    @Override
    public boolean writingIsComplete() {
        return context.writingIsComplete();
    }

    /**
     * Ends the callback scope and prevents a retained method writer from opening another document.
     */
    void close() {
        closed = true;
        activeContext = null;
    }
}
