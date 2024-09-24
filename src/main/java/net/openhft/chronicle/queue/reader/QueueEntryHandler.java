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

package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.queue.internal.reader.InternalMessageToTextQueueEntryHandler;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Handles the processing of queue entries, converting them to text or other forms for consumption.
 * <p>Implements the {@link BiConsumer} interface to consume a {@link WireIn} object, which represents the serialized data,
 * and a {@link Consumer} that processes the resulting string.</p>
 */
public interface QueueEntryHandler extends BiConsumer<WireIn, Consumer<String>>, AutoCloseable {

    /**
     * Closes the handler, releasing any resources held.
     */
    @Override
    void close();

    /**
     * Creates a {@link QueueEntryHandler} that converts messages to text based on the provided {@link WireType}.
     * <p>This is useful when reading queues written in different formats such as binary, JSON, or text.</p>
     *
     * @param wireType The {@link WireType} used to interpret the data
     * @return A {@link QueueEntryHandler} that converts messages to text
     */
    @NotNull
    static QueueEntryHandler messageToText(@NotNull final WireType wireType) {
        return new InternalMessageToTextQueueEntryHandler(wireType);
    }
}
