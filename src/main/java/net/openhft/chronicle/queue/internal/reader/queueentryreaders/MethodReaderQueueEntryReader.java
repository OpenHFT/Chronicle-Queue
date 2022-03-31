package net.openhft.chronicle.queue.internal.reader.queueentryreaders;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.bytes.MethodWriterBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.reader.MessageConsumer;
import net.openhft.chronicle.queue.reader.QueueEntryReader;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;

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
        messageConsumer.consume(tailer.lastReadIndex(), bytes.toString());
        bytes.clear();
        return true;
    }
}
