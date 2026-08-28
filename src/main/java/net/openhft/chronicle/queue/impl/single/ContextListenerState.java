/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodWriterBuilder;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.BinaryMethodWriterInvocationHandler;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.DocumentContextHolder;
import net.openhft.chronicle.wire.MarshallableOut;
import net.openhft.chronicle.wire.VanillaMethodWriterBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/** Queue configuration, appender state and locked output used by context listeners. */
final class ContextListenerState extends DocumentContextHolder implements MarshallableOut {
    static final ContextListenerState UNSET = new ContextListenerState(false);
    static final ContextListenerState NONE = new ContextListenerState(true);

    @Nullable
    private final StoreAppender appender;
    @Nullable
    private final StoreAppender.StoreAppenderContext context;
    @Nullable
    private final Class<?> writerType;
    @Nullable
    private final MarshallableOut.ContextListener<?> listener;
    @Nullable
    private final Object methodWriter;
    private boolean started;
    private Status status = Status.READY;
    private int contextCount = -1;
    @Nullable
    private Throwable failure;
    private int nesting;
    private boolean listenerRolledBack;

    private enum Status {
        READY,
        IN_PROGRESS,
        SUCCEEDED,
        FAILED
    }

    private ContextListenerState(boolean started) {
        this.appender = null;
        this.context = null;
        this.writerType = null;
        this.listener = null;
        this.methodWriter = null;
        this.started = started;
    }

    ContextListenerState(@Nullable Class<?> writerType,
                         @Nullable MarshallableOut.ContextListener<?> listener) {
        this.appender = null;
        this.context = null;
        this.writerType = writerType;
        this.listener = listener;
        this.methodWriter = null;
    }

    private ContextListenerState(@NotNull StoreAppender appender,
                                 @NotNull StoreAppender.StoreAppenderContext context,
                                 @NotNull Class<?> writerType,
                                 @NotNull MarshallableOut.ContextListener<?> listener) {
        this.appender = requireNonNull(appender, "appender");
        this.context = requireNonNull(context, "context");
        this.writerType = null;
        this.listener = requireNonNull(listener, "listener");
        documentContext(context);
        this.methodWriter = methodWriter(requireNonNull(writerType, "writerType"));
    }

    ContextListenerState forAppender(@NotNull StoreAppender appender,
                                     @NotNull StoreAppender.StoreAppenderContext context) {
        return listener == null
                ? UNSET
                : forAppender(appender, context, writerType, listener);
    }

    static ContextListenerState forAppender(@NotNull StoreAppender appender,
                                            @NotNull StoreAppender.StoreAppenderContext context,
                                            @NotNull Class<?> writerType,
                                            @NotNull MarshallableOut.ContextListener<?> listener) {
        return new ContextListenerState(
                appender, context, writerType, listener);
    }

    boolean started() {
        return started;
    }

    boolean requiresDestinationPreflight(boolean metaData) {
        return listener != null && !metaData;
    }

    void onWriteAttempt() {
        if (listener == null)
            return;
        if (status == Status.IN_PROGRESS)
            throw new IllegalStateException("Cannot write to the appender from within a ContextListener; " +
                    "write through the supplied method writer instead");
        started = true;
    }

    boolean beforeDocument(boolean metaData, int actualContextCount) {
        if (listener == null || metaData)
            return false;
        return notifyIfNeeded(actualContextCount);
    }

    boolean beforeRawDocument(int actualContextCount) {
        if (listener == null)
            return false;
        return notifyIfNeeded(actualContextCount);
    }

    private boolean notifyIfNeeded(int actualContextCount) {
        StoreAppender appender = this.appender;
        if (actualContextCount < contextCount)
            throw new IllegalStateException("Queue context count moved backwards from " +
                    contextCount + " to " + actualContextCount);
        if (actualContextCount > contextCount) {
            contextCount = actualContextCount;
            status = Status.READY;
            failure = null;
        }
        if (status == Status.SUCCEEDED)
            return false;
        if (status == Status.FAILED)
            throw new IllegalStateException("The Queue context listener failed for context " +
                    contextCount + "; a later roll is required before application data can be written", failure);
        if (status == Status.IN_PROGRESS)
            throw new IllegalStateException("Queue context listener recursion is not permitted");

        status = Status.IN_PROGRESS;
        listenerRolledBack = false;
        appender.queue().enterContextListenerCallback();
        try {
            try {
                notifyListener();
                if (listenerRolledBack)
                    throw new IllegalStateException("Queue context listener rolled back its output document");
                if (listenerDocumentIsOpen())
                    throw new IllegalStateException("Queue context listener returned with an unclosed document");
                status = Status.SUCCEEDED;
                return true;
            } catch (Throwable callbackFailure) {
                try {
                    rollbackIfNotComplete();
                } catch (Throwable rollbackFailure) {
                    callbackFailure.addSuppressed(rollbackFailure);
                }
                status = Status.FAILED;
                failure = callbackFailure;
                Jvm.warn().on(ContextListenerState.class,
                        "Queue context listener failed: listenerType=" + listener.getClass().getName()
                                + ", appenderIdentity=0x"
                                + Integer.toHexString(System.identityHashCode(appender))
                                + ", contextCount=" + contextCount,
                        callbackFailure);
                throw Jvm.rethrow(callbackFailure);
            }
        } finally {
            nesting = 0;
            appender.queue().exitContextListenerCallback();
        }
    }

    private boolean listenerDocumentIsOpen() {
        return nesting > 0 && context.isOpen();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void notifyListener() {
        ((MarshallableOut.ContextListener) listener).onNewContext(methodWriter);
    }

    @Override
    public DocumentContext writingDocument(boolean metaData) {
        return status == Status.IN_PROGRESS
                ? acquireWritingDocument(metaData)
                : appender.writingDocument(metaData);
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) {
        if (status != Status.IN_PROGRESS)
            return appender.acquireWritingDocument(metaData);

        StoreAppender.StoreAppenderContext context = this.context;
        if (nesting > 0 && context.wire() != null && context.isOpen()) {
            if (!context.chainedElement()) {
                assert metaData == context.isMetaData();
                nesting++;
            }
            return this;
        }

        appender.openContextForContextListener(metaData);
        nesting = 1;
        return this;
    }

    @NotNull
    @Override
    public <T> MethodWriterBuilder<T> methodWriterBuilder(boolean metaData, @NotNull Class<T> type) {
        VanillaMethodWriterBuilder<T> builder = new VanillaMethodWriterBuilder<>(type,
                appender.queue().wireType(),
                () -> new BinaryMethodWriterInvocationHandler(type, metaData,
                        () -> ContextListenerState.this));
        builder.marshallableOut(this);
        builder.metaData(metaData);
        return builder;
    }

    @Override
    public void rollbackIfNotComplete() {
        if (status != Status.IN_PROGRESS) {
            appender.rollbackIfNotComplete();
            return;
        }
        StoreAppender.StoreAppenderContext context = this.context;
        if (nesting == 0 || !context.isOpen())
            return;
        listenerRolledBack = true;
        context.chainedElement(false);
        context.rollbackOnClose();
        nesting = 1;
        close();
    }

    @Override
    public boolean writingIsComplete() {
        return status == Status.IN_PROGRESS
                ? context.writingIsComplete()
                : appender.writingIsComplete();
    }

    @Override
    public void rollbackOnClose() {
        requireCallback();
        listenerRolledBack = true;
        context.rollbackOnClose();
    }

    @Override
    public void close() {
        requireCallback();
        if (nesting == 0)
            throw new IllegalStateException("No ContextListener document is open");
        StoreAppender.StoreAppenderContext context = this.context;
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
        requireCallback();
        context.reset();
        nesting = 0;
    }

    private void requireCallback() {
        if (status != Status.IN_PROGRESS)
            throw new IllegalStateException("ContextListener document is not active");
    }
}
