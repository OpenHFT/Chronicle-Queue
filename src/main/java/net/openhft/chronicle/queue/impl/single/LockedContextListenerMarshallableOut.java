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
import org.jetbrains.annotations.Nullable;

/**
 * A callback-scoped {@link MarshallableOut} that writes through an already-locked
 * {@link StoreAppender}.
 *
 * <p>{@link StoreAppender} invokes a context listener while holding its non-reentrant write lock.
 * Using the appender's public write methods from that callback would attempt to acquire the same
 * lock again. This adapter instead opens documents directly on the locked appender and returns
 * {@link ContextListenerDocumentContext} instances that commit without unlocking it.</p>
 *
 * <p>One adapter and method writer are created per configured appender, outside the write lock.
 * {@link #beginCallback(long)} activates that writer for the callback thread and
 * {@link #endCallback()} invalidates it again. A retained writer therefore cannot write between
 * callbacks or from another thread, while later roll callbacks avoid reconstructing the method
 * writer.</p>
 */
final class LockedContextListenerMarshallableOut implements MarshallableOut {
    private final StoreAppender appender;
    private final StoreAppender.StoreAppenderContext context;
    private final WireType wireType;
    private long safeLength;
    @Nullable
    private ContextListenerDocumentContext activeContext;
    @Nullable
    private volatile Thread callbackThread;

    /**
     * Creates the reusable, appender-bound output used to build one method writer.
     *
     * @param appender appender whose write lock is held during callbacks
     * @param context  appender context used for listener documents
     * @param wireType wire type used to construct the listener's method writer
     */
    LockedContextListenerMarshallableOut(StoreAppender appender,
                                         StoreAppender.StoreAppenderContext context,
                                         WireType wireType) {
        this.appender = appender;
        this.context = context;
        this.wireType = wireType;
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
        requireCallbackThread();
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
        requireCallbackThread();
        context.rollbackIfNotComplete();
    }

    @Override
    public boolean writingIsComplete() {
        requireCallbackThread();
        return context.writingIsComplete();
    }

    /**
     * Activates the prebuilt method writer for one callback on the current thread.
     *
     * @param safeLength maximum number of bytes that may be written without overlapping a mapping
     */
    void beginCallback(long safeLength) {
        if (callbackThread != null)
            throw new IllegalStateException("ContextListener method writer is already in a callback");
        this.safeLength = safeLength;
        activeContext = null;
        callbackThread = Thread.currentThread();
    }

    /** Ends the callback scope and invalidates the method writer until the next callback. */
    void endCallback() {
        activeContext = null;
        callbackThread = null;
    }

    private void requireCallbackThread() {
        if (callbackThread != Thread.currentThread())
            throw new IllegalStateException(
                    "ContextListener method writer can only be used by the active callback thread");
    }
}
