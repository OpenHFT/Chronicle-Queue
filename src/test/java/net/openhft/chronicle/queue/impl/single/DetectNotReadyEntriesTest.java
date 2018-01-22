/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;

/*
 * Created by Peter Lawrey on 19/05/16.
 */
public class DetectNotReadyEntriesTest {

    static {
        // init class
        SingleChronicleQueueBuilder.init();
    }

    @Test
    public void testDeadEntries() throws FileNotFoundException {
        // TODO FIX.
        if (OS.isWindows())
            return;
        File dir = new File(OS.TARGET, getClass().getSimpleName() + "-" + System.nanoTime());
        dir.mkdir();

        MappedBytes bytes = MappedBytes.mappedBytes(new File(dir, "19700101" + SingleChronicleQueue.SUFFIX), 64 << 10);
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
        // make it incomplete, note that the length is removed,
        // since writing a length into an incomplete excerpt is not allowed
        wire.bytes().writeInt(pos, Wires.NOT_COMPLETE);

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
                "...\n" +
                "# 17 bytes remaining\n", Wires.fromSizePrefixedBlobs(bytes.readPosition(0)));

        bytes.release();

        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(dir)
                .testBlockSize()
                .build()) {

            queue.acquireAppender().writeText("Bye for now");
        }

        try {
            IOTools.shallowDeleteDirWithFiles(dir.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
