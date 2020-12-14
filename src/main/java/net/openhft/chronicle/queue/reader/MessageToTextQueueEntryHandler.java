package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;

import java.util.function.Consumer;

@Deprecated /* For removal in x.22, Use QueueEntryHandler.messageToText() instead */
public final class MessageToTextQueueEntryHandler implements QueueEntryHandler {
    private final Bytes textConversionTarget = Bytes.elasticByteBuffer();
    private final WireType wireType;

    public MessageToTextQueueEntryHandler(WireType wireType) {
        this.wireType = wireType;
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