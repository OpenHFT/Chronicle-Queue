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

package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.reader.QueueEntryHandler;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

/**
 * The {@code InternalDummyMethodReaderQueueEntryHandler} is a dummy implementation of the {@link QueueEntryHandler} interface
 * for processing method reader entries from a queue.
 * <p>
 * It converts binary wire entries into a specified wire type (e.g., text) and passes the result to the message handler.
 * This implementation is particularly useful when you need to process queue entries as a text representation.
 */
public final class InternalDummyMethodReaderQueueEntryHandler implements QueueEntryHandler {
    private final Bytes<?> textConversionTarget = Bytes.allocateElasticOnHeap();  // Bytes object for storing the text representation
    private final WireType wireType;  // The wire type for converting binary wire entries

    /**
     * Constructs an {@code InternalDummyMethodReaderQueueEntryHandler} with the specified {@link WireType}.
     *
     * @param wireType The wire type to be used for converting entries, must not be null
     */
    public InternalDummyMethodReaderQueueEntryHandler(@NotNull WireType wireType) {
        this.wireType = requireNonNull(wireType);  // Ensures that the wire type is not null
    }

    /**
     * Processes entries from the given {@link WireIn}, converting them to the specified wire type and passing
     * the result to the provided {@code messageHandler}.
     * <p>
     * This method reads the binary wire entries, converts them to the target format, and passes the result to the
     * message handler every two entries (i.e., after every second entry).
     *
     * @param wireIn        The wire input to process
     * @param messageHandler The handler that processes the converted message
     */
    @Override
    public void accept(final WireIn wireIn, final Consumer<String> messageHandler) {
        long elementCount = 0;
        while (wireIn.hasMore()) {
            // Convert binary wire entries into the specified wire type and store in textConversionTarget
            new BinaryWire(wireIn.bytes()).copyOne(wireType.apply(textConversionTarget));

            elementCount++;
            // Every two elements, pass the converted text to the message handler and clear the buffer
            if ((elementCount & 1) == 0) {
                messageHandler.accept(textConversionTarget.toString());
                textConversionTarget.clear();
            }
        }
    }

    /**
     * Releases the resources used by this entry handler, particularly the {@code textConversionTarget} buffer.
     */
    @Override
    public void close() {
        textConversionTarget.releaseLast();  // Release the memory used by the buffer
    }
}
