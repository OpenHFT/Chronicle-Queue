/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.queue.ChronicleQueue;

import java.io.Closeable;

/**
 * A {@code Pretoucher} is designed to be called from a long-lived thread to pre-touch pages in the Chronicle Queue's store file.
 * It is typically used to keep ahead of appenders and ensure that pages are ready to be written to, improving write performance.
 * <p>
 * To get an instance of a {@code Pretoucher}, call {@link ChronicleQueue#createPretoucher()} on a Chronicle Queue instance.
 * </p>
 * <p>
 * The {@code execute()} method performs the pre-touching operation. If the underlying queue is closed, invoking this method
 * will throw an {@link InvalidEventHandlerException}. Resources are automatically released when the queue is closed, but you can also call {@code close()}
 * manually to release resources.
 * </p>
 */
public interface Pretoucher extends Closeable {

    /**
     * Executes the pre-touching process, advancing over pages in the Chronicle Queue's store file.
     * This method is intended to be run continuously in a background thread, ensuring pages are prepared
     * before they are accessed by appenders.
     * <p>
     * If the underlying queue has been closed, this method will throw an {@link InvalidEventHandlerException}.
     * </p>
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
