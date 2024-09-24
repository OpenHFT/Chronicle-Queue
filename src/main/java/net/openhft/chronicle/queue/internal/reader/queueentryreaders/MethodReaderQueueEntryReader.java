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

package net.openhft.chronicle.queue.internal.reader.queueentryreaders;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.bytes.MethodWriterBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.reader.MessageConsumer;
import net.openhft.chronicle.queue.reader.QueueEntryReader;
import net.openhft.chronicle.wire.*;

import java.nio.ByteBuffer;

/**
 * {@code MethodReaderQueueEntryReader} is a queue entry reader that processes method calls from a queue using a {@link MethodReader}.
 * <p>
 * It reads and decodes method calls from a queue using a {@link WireType}, and forwards them to a {@link MessageConsumer}.
 * This class supports optional message history logging.
 */
public final class MethodReaderQueueEntryReader implements QueueEntryReader {

    private final ExcerptTailer tailer;  // The ExcerptTailer used to read from the queue
    private final MessageConsumer messageConsumer;  // The message consumer that processes the read messages
    private final MethodReader methodReader;  // The method reader that reads method calls from the queue
    private final Bytes<ByteBuffer> bytes;  // A buffer for holding the serialized message

    /**
     * Constructs a {@code MethodReaderQueueEntryReader} with the provided tailer, message consumer, wire type, and method reader interface.
     * Optionally logs message history if specified.
     *
     * @param tailer                The {@link ExcerptTailer} used to read entries from the queue
     * @param messageConsumer       The {@link MessageConsumer} that handles the consumed messages
     * @param wireType              The {@link WireType} used to serialize/deserialize the method calls
     * @param methodReaderInterface The interface used to define the methods to be read from the queue
     * @param showMessageHistory    Whether to include message history in the output
     */
    public MethodReaderQueueEntryReader(ExcerptTailer tailer, MessageConsumer messageConsumer, WireType wireType,
                                        Class<?> methodReaderInterface, boolean showMessageHistory) {
        this.tailer = tailer;
        this.messageConsumer = messageConsumer;
        bytes = Bytes.elasticHeapByteBuffer(256);  // Allocate a buffer for holding serialized data
        Wire wire = wireType.apply(bytes);
        if (wire instanceof TextWire)
            ((TextWire) wire).useTextDocuments();  // Use text documents if it's a TextWire

        // Build the MethodWriter from the provided method reader interface
        MethodWriterBuilder<?> mwb = wire.methodWriterBuilder(methodReaderInterface);
        if (showMessageHistory) {
            // If message history is enabled, log message history details
            mwb.updateInterceptor((methodName, t) -> {
                MessageHistory messageHistory = MessageHistory.get();
                // this is an attempt to recognise that no MH was read and instead the method reader called reset(...) on it
                if (messageHistory.sources() != 1 || messageHistory.timings() != 1)
                    bytes.append(messageHistory + System.lineSeparator());
                return true;
            });
        }
        // Initialize the method reader
        methodReader = tailer.methodReader(mwb.build());
    }

    /**
     * Reads and processes one method call from the queue.
     * <p>
     * If a method call is successfully read, it is passed to the {@link MessageConsumer} along with the last read index.
     *
     * @return {@code true} if a method call was read and processed, {@code false} otherwise
     */
    @Override
    public boolean read() {
        if (!methodReader.readOne()) {
            return false;  // No method call to read
        }
        // Consume the read message and pass it to the message consumer
        messageConsumer.consume(tailer.lastReadIndex(), bytes.toString());
        bytes.clear();  // Clear the buffer for the next message
        return true;
    }
}
