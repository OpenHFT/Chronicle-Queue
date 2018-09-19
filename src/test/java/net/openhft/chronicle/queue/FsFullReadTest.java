package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.queue.impl.CommonStore;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireDumper;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;

import static java.lang.System.err;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/*
 * Created by AM on 3/20/17. The specimen cq file was created when it hit the filesystem full.
 * The standard DC present code can read only 1 entry, while the dumper can read 31k entries.
 * It would be great if the normal code could read most of the entries also.
 */
public class FsFullReadTest {

    @NotNull
    private static String basePath = "src/test/resources/tr2";

    @Ignore("broken test")
    @Test
    public void testFullReadFs() throws Exception {

        RollingChronicleQueue queue = SingleChronicleQueueBuilder.binary(basePath)
                .blockSize(256 << 1000)
                .rollCycle(RollCycles.DAILY)
                .build();
        ExcerptTailer tailer = queue.createTailer();
        DocumentContext dc = tailer.readingDocument();
        boolean doExit = false;
        int entries = 0;
        while (!doExit) {
            try {
                if (dc.isPresent()) {
                    entries++;
                    Wire w = dc.wire();
                    LocalDateTime dt = w.read().dateTime();
                    assertNotNull(dt);
                    byte[] b = w.read().bytes();
                    assertEquals(1024, b.length);
                } else {
                    System.out.println("Exiting");
                    doExit = true;
                }
            } finally {
                dc.close();
            }
        }
        System.out.println(String.format("Read %d entries.", entries));
        CommonStore commonStore = queue.storeForCycle(queue.cycle(), 0, false);
        File file = commonStore.file();
        queue.close();
        int dumpEntries = 0;
        try {
            MappedBytes bytes = MappedBytes.mappedBytes(file, 4 << 20);
            bytes.readLimit(bytes.realCapacity());

            WireDumper dumper = WireDumper.of(bytes);
            Bytes<ByteBuffer> buffer = Bytes.elasticByteBuffer();
            while (bytes.readRemaining() >= 4) {
                StringBuilder sb = new StringBuilder();
                boolean last = dumper.dumpOne(sb, buffer);
                assertTrue(sb.length() > 0);

                if (last)
                    break;
                dumpEntries++;
            }
        } catch (IOException ioe) {
            err.println("Failed to read " + file + " " + ioe);
        }

        assertEquals(dumpEntries, entries);
    }

}
