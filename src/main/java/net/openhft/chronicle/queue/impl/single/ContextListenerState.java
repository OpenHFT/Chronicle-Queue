/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MethodWriterBuilder;
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
    private boolean notifying;
    private int lastContextCount = -1;
    private int nesting;

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

    void onWriteAttempt() {
        if (listener == null)
            return;
        if (notifying)
            throw new IllegalStateException("Cannot write to the appender from within a ContextListener; " +
                    "write through the supplied method writer instead");
        started = true;
    }

    boolean beforeDocument(boolean metaData) {
        if (listener == null || metaData)
            return false;
        return notifyIfNeeded();
    }

    boolean beforeRawDocument() {
        if (listener == null)
            return false;
        appender.resetPositionForContextListener();
        return notifyIfNeeded();
    }

    private boolean notifyIfNeeded() {
        StoreAppender appender = this.appender;
        int contextCount = appender.cycle();
        if (contextCount <= lastContextCount)
            return false;

        notifying = true;
        try {
            try {
                notifyListener();
            } finally {
                rollbackIfNotComplete();
            }
            // A listener document can encounter EOF and move the ordinary appender to the next
            // roll. Latch the actual destination so the following application write does not
            // notify the same context again under its new cycle.
            lastContextCount = appender.cycle();
            return true;
        } finally {
            nesting = 0;
            notifying = false;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void notifyListener() {
        ((MarshallableOut.ContextListener) listener).onNewContext(methodWriter);
    }

    @Override
    public DocumentContext writingDocument(boolean metaData) {
        return notifying
                ? acquireWritingDocument(metaData)
                : appender.writingDocument(metaData);
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) {
        if (!notifying)
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
        if (!notifying) {
            appender.rollbackIfNotComplete();
            return;
        }
        StoreAppender.StoreAppenderContext context = this.context;
        if (nesting == 0 || !context.isOpen())
            return;
        context.chainedElement(false);
        context.rollbackOnClose();
        nesting = 1;
        close();
    }

    @Override
    public boolean writingIsComplete() {
        return notifying
                ? context.writingIsComplete()
                : appender.writingIsComplete();
    }

    @Override
    public void rollbackOnClose() {
        requireCallback();
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
        if (!notifying)
            throw new IllegalStateException("ContextListener document is not active");
    }
}
