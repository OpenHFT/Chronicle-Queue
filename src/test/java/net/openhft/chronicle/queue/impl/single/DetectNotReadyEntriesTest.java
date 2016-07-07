package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.*;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;

/**
 * Created by peter on 19/05/16.
 */
public class DetectNotReadyEntriesTest {
    private static final int TEST_CHUNK_SIZE = 64 * 1024;

    static {
        // init class
        SingleChronicleQueueBuilder.init();
    }

    @Test
    public void testDeadEntries() throws FileNotFoundException {
        File dir = new File(OS.TARGET + "/deleteme-" + System.nanoTime());
        dir.mkdir();

        MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), TEST_CHUNK_SIZE);
        Wire wire = new BinaryWire(bytes);
        try (DocumentContext dc = wire.writingDocument(true)) {
            dc.wire().writeEventName(() -> "header").typePrefix(SingleChronicleQueueStore.class).marshallable(w -> {
                w.write(() -> "wireType").object(WireType.BINARY);
                w.write(() -> "writePosition").int64forBinding(288 + 4 + 17);
                w.write(() -> "roll").typedMarshallable(new SCQRoll(RollCycles.DAILY, 0));
                w.write(() -> "indexing").typedMarshallable(new SCQIndexing(WireType.BINARY, 32 << 10, 32));
                w.write(() -> "lastAcknowledgedIndexReplicated").int64forBinding(0);
            });
        }

        long pos = wire.bytes().writePosition();
        try (DocumentContext dc = wire.writingDocument(false)) {
            dc.wire().write("test").text("Hello World");
        }
        assertEquals(17, wire.bytes().readInt(pos));
        // make it incomplete.
        wire.bytes().writeInt(pos, Wires.NOT_COMPLETE | 17);

        assertEquals("--- !!meta-data #binary\n" +
                "header: !SCQStore {\n" +
                "  wireType: !WireType BINARY,\n" +
                "  writePosition: 309,\n" +
                "  roll: !SCQSRoll {\n" +
                "    length: !int 86400000,\n" +
                "    format: yyyyMMdd,\n" +
                "    epoch: 0\n" +
                "  },\n" +
                "  indexing: !SCQSIndexing {\n" +
                "    indexCount: !int 32768,\n" +
                "    indexSpacing: 32,\n" +
                "    index2Index: 0,\n" +
                "    lastIndex: 0\n" +
                "  },\n" +
                "  lastAcknowledgedIndexReplicated: 0\n" +
                "}\n" +
                "# position: 288, header: -1 or 0\n" +
                "--- !!not-ready-data! #binary\n" +
                "test: Hello World\n", Wires.fromSizePrefixedBlobs(bytes.readPosition(0)));

        bytes.close();

        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .blockSize(TEST_CHUNK_SIZE)
                .build();
// TODO Fix the last entry.
        queue.close();
        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
