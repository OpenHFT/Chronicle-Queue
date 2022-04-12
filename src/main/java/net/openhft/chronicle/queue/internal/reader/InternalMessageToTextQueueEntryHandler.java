package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.reader.QueueEntryHandler;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;

import java.util.function.Consumer;

import static net.openhft.chronicle.core.util.ObjectUtils.requireNonNull;

public final class InternalMessageToTextQueueEntryHandler implements QueueEntryHandler {
    private final Bytes<?> textConversionTarget = Bytes.elasticByteBuffer();
    private final WireType wireType;

    public InternalMessageToTextQueueEntryHandler(WireType wireType) {
        this.wireType = requireNonNull(wireType);
    }

    private static boolean isBinaryFormat(final byte dataFormatIndicator) {
        return dataFormatIndicator < 0;
    }

    @Override
    public void accept(final WireIn wireIn, final Consumer<String> messageHandler) {
        final Bytes<?> serialisedMessage = wireIn.bytes();
        final byte dataFormatIndicator = serialisedMessage.readByte(serialisedMessage.readPosition());
        String text;

        if (isBinaryFormat(dataFormatIndicator)) {
            textConversionTarget.clear();
            final BinaryWire binaryWire = new BinaryWire(serialisedMessage);
            binaryWire.copyTo(wireType.apply(textConversionTarget));
            text = textConversionTarget.toString();
        } else {
            text = serialisedMessage.toString();
        }

        messageHandler.accept(text);
    }

    @Override
    public void close() {
        textConversionTarget.releaseLast();
    }
}