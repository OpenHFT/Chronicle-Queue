package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;

import java.io.File;
import java.util.Arrays;

/**
 * see https://github.com/OpenHFT/Chronicle-Queue/issues/535
 * Created by Rob Austin
 */
public class MappedFileSafeLimitTooSmallTest extends ChronicleQueueTestBase {

    @org.junit.Test
    public void testMappedFileSafeLimitTooSmall() {

        final int arraySize = 40_000;
        final int blockSize = arraySize * 6;
        byte[] data = new byte[arraySize];
        Arrays.fill(data, (byte) 'x');
        File tmpDir = getTmpDir();

        try (final ChronicleQueue queue =
                     SingleChronicleQueueBuilder.builder(tmpDir, WireType.BINARY).blockSize(blockSize).build()) {

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = queue.acquireAppender().writingDocument()) {
                   // System.out.println(dc.wire().bytes().writeRemaining());
                    dc.wire().write("data").bytes(data);
                }
            }
        }

        try (final ChronicleQueue queue =
                     SingleChronicleQueueBuilder.builder(tmpDir, WireType.BINARY).blockSize(blockSize).build()) {

            for (int i = 0; i < 5; i++) {
                try (DocumentContext dc = queue.createTailer().readingDocument()) {
                    Assert.assertArrayEquals(data, dc.wire().read("data").bytes());
                }
            }
 }
    }

}
