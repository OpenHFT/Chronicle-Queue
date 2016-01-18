package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.ringbuffer.BytesRingBuffer;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

import static net.openhft.chronicle.bytes.NativeBytesStore.nativeStoreWithFixedCapacity;

/**
 * @author Rob Austin.
 */
public class BufferAppender implements ExcerptAppender {

    private final BytesRingBuffer ringBuffer = new BytesRingBuffer(nativeStoreWithFixedCapacity
            (64 << 10));
    private final ExcerptAppender underlyingAppender;
    private final Wire wire;

    public BufferAppender(@NotNull final EventLoop eventLoop,
                          @NotNull final ExcerptAppender underlyingAppender) {

        final ReadBytesMarshallable readBytesMarshallable = bytes -> {
            try {
                underlyingAppender.writeBytes(bytes);
            } catch (IOException e) {
                throw Jvm.rethrow(e);
            }
        };

        eventLoop.addHandler(() -> {
                    try {
                        long size = ringBuffer.read(readBytesMarshallable);
                        return size > 0;
                    } catch (InterruptedException e) {
                        throw Jvm.rethrow(e);
                    }
                }
        );

        eventLoop.start();
        this.underlyingAppender = underlyingAppender;
        this.wire = underlyingAppender.queue().wireType().apply(Bytes.elasticByteBuffer());
    }

    @Override
    public long writeDocument(@NotNull WriteMarshallable writer) throws IOException {
        final Bytes<?> bytes = wire.bytes();
        bytes.clear();
        writer.writeMarshallable(wire);
        return writeBytes(bytes);
    }

    @Override
    public long writeBytes(@NotNull WriteBytesMarshallable marshallable) throws IOException {
        final Bytes<?> bytes = wire.bytes();
        bytes.clear();
        marshallable.writeMarshallable(bytes);
        return writeBytes(bytes);
    }

    @Override
    public long writeBytes(@NotNull Bytes<?> bytes) throws IOException {
        try {
            ringBuffer.offer(bytes);
        } catch (InterruptedException e) {
            throw Jvm.rethrow(e);
        }

        return -1;
    }

    @Override
    public long index() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public long cycle() {
        return underlyingAppender.cycle();
    }

    @Override
    public ExcerptAppender underlying() {
        return underlyingAppender;
    }

    @Override
    public ChronicleQueue queue() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void prefetch() {
    }

}
