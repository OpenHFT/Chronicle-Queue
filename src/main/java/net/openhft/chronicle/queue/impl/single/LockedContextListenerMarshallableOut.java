/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodWriterBuilder;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.BinaryMethodWriterInvocationHandler;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.WriteDocumentContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A callback-scoped {@link MarshallableOut} that writes through an already-locked
 * {@link StoreAppender}.
 *
 * <p>{@link StoreAppender} invokes a context listener while holding its non-reentrant write lock.
 * Using the appender's public write methods from that callback would attempt to acquire the same
 * lock again. This adapter instead opens documents directly on the locked appender and also acts
 * as their {@link WriteDocumentContext}, committing them without unlocking the appender.</p>
 *
 * <p>One adapter and method writer are created per configured appender, outside the write lock.
 * {@link #beginCallback(long)} activates that writer for the callback thread and
 * {@link #endCallback()} invalidates it again. A retained writer therefore cannot write between
 * callbacks or from another thread, while later roll callbacks avoid reconstructing the method
 * writer. Combining the output and document-context roles avoids allocating a delegating wrapper
 * for every listener document.</p>
 *
 * <p>This bridge remains separate from {@link ActiveAppenderContextListenerLifecycle}: the
 * lifecycle decides <em>when</em> to notify and owns the listener, while this object is the stable
 * {@code MarshallableOut} captured by the cached method writer and defines <em>how</em> that writer
 * uses the already-held appender lock. Using the appender itself as the writer target would try to
 * acquire its non-reentrant lock again.</p>
 */
final class LockedContextListenerMarshallableOut implements MarshallableOut, WriteDocumentContext {
    private final StoreAppender appender;
    private final StoreAppender.StoreAppenderContext context;
    private final WireType wireType;
    private long safeLength;
    private int nesting;
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
        if (nesting > 0 && context.wire() != null && context.isOpen()) {
            if (!context.chainedElement()) {
                assert metaData == context.isMetaData();
                nesting++;
            }
            return this;
        }

        appender.openContextForContextListener(metaData, safeLength);
        nesting = 1;
        return this;
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
        if (nesting == 0 || !context.isOpen())
            return;
        context.chainedElement(false);
        context.rollbackOnClose();
        nesting = 1;
        close();
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
        nesting = 0;
        callbackThread = Thread.currentThread();
    }

    /**
     * Ends the callback scope, rolling back an unclosed document before invalidating the writer.
     */
    void endCallback() {
        try {
            rollbackIfNotComplete();
        } finally {
            nesting = 0;
            callbackThread = null;
        }
    }

    @Override
    public void start(boolean metaData) {
        context.start(metaData);
    }

    @Override
    public boolean chainedElement() {
        return context.chainedElement();
    }

    @Override
    public void chainedElement(boolean chainedElement) {
        context.chainedElement(chainedElement);
    }

    @Override
    public boolean isEmpty() {
        return context.isEmpty();
    }

    @Override
    public boolean isMetaData() {
        return context.isMetaData();
    }

    @Override
    public boolean isPresent() {
        return context.isPresent();
    }

    @Nullable
    @Override
    public Wire wire() {
        return context.wire();
    }

    @Override
    public boolean isNotComplete() {
        return context.isNotComplete();
    }

    @Override
    public void rollbackOnClose() {
        context.rollbackOnClose();
    }

    /**
     * Closes one listener document scope, committing only when its outermost scope closes.
     * Chained method-writer elements remain open until the terminating method clears the chain.
     */
    @Override
    public void close() {
        requireCallbackThread();
        if (nesting == 0)
            throw new IllegalStateException("No ContextListener document is open");
        if (context.chainedElement())
            return;
        if (nesting > 1) {
            nesting--;
            return;
        }
        nesting = 0;
        appender.closeContextForContextListener();
    }

    @Override
    public void reset() {
        context.reset();
        nesting = 0;
    }

    @Override
    public int sourceId() {
        return context.sourceId();
    }

    @Override
    public long index() throws IORuntimeException {
        return context.index();
    }

    @Override
    public long contextCount() {
        return context.contextCount();
    }

    private void requireCallbackThread() {
        if (callbackThread != Thread.currentThread())
            throw new IllegalStateException(
                    "ContextListener method writer can only be used by the active callback thread");
    }
}
