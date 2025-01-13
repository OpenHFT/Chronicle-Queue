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

public final class MethodReaderQueueEntryReader implements QueueEntryReader {

    private final ExcerptTailer tailer;
    private final MessageConsumer messageConsumer;
    private final MethodReader methodReader;
    private final Bytes<ByteBuffer> bytes;

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

    @Override
    public boolean read() {
        if (!methodReader.readOne()) {
            return false;
        }
        if (bytes.isEmpty()) {
            // we read something from the queue but the MR filtered it i.e. did not dispatch
            return false;
        }
        messageConsumer.consume(tailer.lastReadIndex(), bytes.toString());
        bytes.clear();
        return true;
    }
}
