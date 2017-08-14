package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.TextWire;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class MessageToTextQueueEntryHandler implements BiConsumer<Bytes<?>, Consumer<String>>, AutoCloseable {
    private final Bytes textConversionTarget = Bytes.elasticByteBuffer();

    @Override
    public void accept(final Bytes<?> serialisedMessage, final Consumer<String> messageHandler) {
        final byte dataFormatIndicator = serialisedMessage.readByte(serialisedMessage.readPosition());
        String text;

        if (isBinaryFormat(dataFormatIndicator)) {
            textConversionTarget.clear();
            final BinaryWire binaryWire = new BinaryWire(serialisedMessage);
            binaryWire.copyTo(new TextWire(textConversionTarget));
            text = textConversionTarget.toString();
        } else {
            text = serialisedMessage.toString();
        }

        messageHandler.accept(text);
    }

    @Override
    public void close() {
        textConversionTarget.release();
    }

    private static boolean isBinaryFormat(final byte dataFormatIndicator) {
        return dataFormatIndicator < 0;
    }
}