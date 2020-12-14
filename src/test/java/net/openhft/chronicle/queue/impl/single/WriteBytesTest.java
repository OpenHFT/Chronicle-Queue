package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public final class WriteBytesTest extends QueueTestCommon {
    private static final byte[] PAYLOAD = {0x7E, 0x42, 0x19, 0x37};

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void shouldWriteBytes() throws IOException {
        // test was not compiling see   http://teamcity.chronicle.software/viewType.html?buildTypeId=OpenHFT_ChronicleQueue4_Snapshot

    }

     // @Test
  /*  public void shouldWriteBytes() throws IOException {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.
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
    }*/
}