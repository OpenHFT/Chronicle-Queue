/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WriteDocumentContext;
import org.jetbrains.annotations.Nullable;

/**
 * Presents a {@link StoreAppender.StoreAppenderContext} to a context-listener method writer.
 *
 * <p>Most operations delegate directly to the appender's regular document context. Closing is the
 * important exception: the listener runs after {@link StoreAppender} has acquired its non-reentrant
 * write lock, so the listener document must be committed without releasing that lock. The outer
 * application document is opened immediately afterwards and remains responsible for the eventual
 * unlock.</p>
 *
 * <p>Instances are created by {@link LockedContextListenerMarshallableOut} and are intended to live
 * only for the duration of a
 * {@link net.openhft.chronicle.wire.MarshallableOut.ContextListener#onNewContext(Object)}
 * callback.</p>
 */
final class ContextListenerDocumentContext implements WriteDocumentContext {
    private final StoreAppender appender;
    private final WriteDocumentContext delegate;

    /**
     * Creates a listener-facing view of the appender's active document context.
     *
     * @param appender appender that owns the already-held write lock
     * @param delegate appender context to which document state is delegated
     */
    ContextListenerDocumentContext(StoreAppender appender, WriteDocumentContext delegate) {
        this.appender = appender;
        this.delegate = delegate;
    }

    @Override
    public void start(boolean metaData) {
        delegate.start(metaData);
    }

    @Override
    public boolean chainedElement() {
        return delegate.chainedElement();
    }

    @Override
    public void chainedElement(boolean chainedElement) {
        delegate.chainedElement(chainedElement);
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean isMetaData() {
        return delegate.isMetaData();
    }

    @Override
    public boolean isPresent() {
        return delegate.isPresent();
    }

    @Nullable
    @Override
    public Wire wire() {
        return delegate.wire();
    }

    @Override
    public boolean isNotComplete() {
        return delegate.isNotComplete();
    }

    @Override
    public void rollbackOnClose() {
        delegate.rollbackOnClose();
    }

    @Override
    public void close() {
        appender.closeContextForContextListener();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public int sourceId() {
        return delegate.sourceId();
    }

    @Override
    public long index() throws IORuntimeException {
        return delegate.index();
    }

    @Override
    public long contextCount() {
        return delegate.contextCount();
    }

    @Override
    public void rollbackIfNotComplete() {
        delegate.rollbackIfNotComplete();
    }
}
