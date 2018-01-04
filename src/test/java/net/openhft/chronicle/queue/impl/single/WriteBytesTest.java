package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.HeapBytesStore;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public final class WriteBytesTest {
    private static final byte[] PAYLOAD = {0x7E, 0x42, 0x19, 0x37};

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void shouldWriteBytes() throws IOException {
        try (final SingleChronicleQueue queue = SingleChronicleQueueBuilder.
                binary(tmpDir.newFolder()).testBlockSize().build()) {
            final ExcerptAppender appender = queue.acquireAppender();

            final HeapBytesStore<ByteBuffer> store = HeapBytesStore.uninitialized();
            store.init(ByteBuffer.wrap(PAYLOAD));
            appender.writeBytes(store);

            final ExcerptTailer tailer = queue.createTailer();
            final HeapBytesStore<byte[]> copy = HeapBytesStore.uninitialized();
            copy.init(new byte[4]);
            tailer.readBytes(copy.bytesForWrite());
            assertTrue(Arrays.equals(PAYLOAD, copy.underlyingObject()));
        }
    }
}