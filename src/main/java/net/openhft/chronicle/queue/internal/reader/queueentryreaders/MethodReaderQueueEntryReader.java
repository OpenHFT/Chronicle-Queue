/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
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

    private final ExcerptTailer tailer;
    private final MessageConsumer messageConsumer;
    private final MethodReader methodReader;
    private final Bytes<ByteBuffer> bytes;

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
    public MethodReaderQueueEntryReader(ExcerptTailer tailer, MessageConsumer messageConsumer, WireType wireType, Class<?> methodReaderInterface, boolean showMessageHistory) {
        this.tailer = tailer;
        this.messageConsumer = messageConsumer;
        bytes = Bytes.elasticHeapByteBuffer(256);
        Wire wire = wireType.apply(bytes);
        if (wire instanceof TextWire)
            ((TextWire) wire).useTextDocuments();
        MethodWriterBuilder<?> mwb = wire.methodWriterBuilder(methodReaderInterface);
        if (showMessageHistory)
            mwb.updateInterceptor((methodName, t) -> {
                MessageHistory messageHistory = MessageHistory.get();
                // this is an attempt to recognise that no MH was read and instead the method reader called reset(...) on it
                if (messageHistory.sources() != 1 || messageHistory.timings() != 1)
                    bytes.append(messageHistory + System.lineSeparator());
                return true;
            });
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
            return false;
        }
        if (bytes.isEmpty()) {
            // we read something from the queue but the MR filtered it i.e. did not dispatch
            return true;
        }
        messageConsumer.consume(tailer.lastReadIndex(), bytes.toString());
        bytes.clear();
        return true;
    }
}
