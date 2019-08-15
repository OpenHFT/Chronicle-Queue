package net.openhft.chronicle.queue.bench;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.batch.BatchAppenderNative;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by Rob Austin
 * <p>
 * see Chronicle-Queue/c++/src/BatchAppenderNative.cpp
 */
public class BatchAppenderNativeTest {

    @Test
    public void testNative() {
        if (!OS.isMacOSX())
            return;

        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        try {

            BatchAppenderNative batchAppenderNative = new BatchAppenderNative();

            // this will append a message in wire of hello world
            long result = batchAppenderNative.writeMessages(bytes.addressForWrite(0), bytes.realCapacity(), 1);

            int len = (int) result;
            int count = (int) (result >> 32);
            bytes.readLimit(len);

            Assert.assertEquals(16, len);
            Assert.assertEquals(1, count);

            Wire w = WireType.BINARY.apply(bytes);

            for (int i = 0; i < count; i++) {
                try (DocumentContext dc = w.readingDocument()) {
                    Assert.assertEquals("hello world", dc.wire().getValueIn().text());
                }
            }
        } finally {
            bytes.release();
        }
    }
}
