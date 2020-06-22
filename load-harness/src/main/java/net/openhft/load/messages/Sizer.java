package net.openhft.load.messages;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.AbstractMarshallable;

import java.nio.ByteBuffer;

public final class Sizer {
    public static int size(final BytesMarshallable message) {
        final Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer();
        try {
            message.writeMarshallable(buffer);
            return (int) buffer.writePosition();
        } finally {
            buffer.releaseLast();
        }
    }
}
