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

    void execute() throws InvalidEventHandlerException;

    @Override
    void close();
}
