/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.ChronicleQueue;

import java.io.Closeable;

/**
 * A class designed to be called from a long-lived thread.
 * To get an instance of a Pretoucher, call {@link ChronicleQueue#createPretoucher()}
 * <p>
 * Upon invocation of the {@code execute()} method, this object will pre-touch pages in the supplied queue's underlying store file, attempting to keep
 * ahead of any appenders to the queue.
 * <p>
 * Resources held by this object will be released when the underlying queue is closed.
 * <p>
 * Alternatively, the {@code close()} method can be called to close the supplied queue and release any other resources. Invocation of the {@code
 * execute()} method after {@code close()} has been called will cause an {@code InvalidEventHandlerException} to be thrown.
 */
public interface Pretoucher extends Closeable {

    /**
     * Executes the pre-touching process, advancing over pages in the Chronicle Queue's store file.
     * This method is intended to be run continuously in a background thread, ensuring pages are prepared
     * before they are accessed by appenders.
     * <p>
     * If the underlying queue has been closed, this method will throw an {@link InvalidEventHandlerException}.
     *
     *
     * @throws InvalidEventHandlerException if the queue has been closed or if there is an issue during the pre-touch operation.
     */
    void execute() throws InvalidEventHandlerException;

    /**
     * Closes the pretoucher and releases any resources associated with it.
     * After calling this method, further calls to {@link #execute()} will throw an {@link InvalidEventHandlerException}.
     */
    @Override
    void close();
}
