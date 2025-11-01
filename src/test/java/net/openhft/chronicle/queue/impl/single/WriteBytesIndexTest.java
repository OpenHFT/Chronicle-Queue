/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_SECONDLY;
import static org.junit.Assert.*;

public class WriteBytesIndexTest extends QueueTestCommon {
    @Test
    public void writeMultipleAppenders() {
        File path = IOTools.createTempFile("writeMultipleAppenders");
        try (ChronicleQueue q0 = createQueue(path);
             ExcerptAppender a0 = q0.createAppender();
             ExcerptTailer t0 = q0.createTailer();

             ChronicleQueue q1 = createQueue(path);
             ExcerptAppender a1 = q1.createAppender();
             ExcerptTailer t1 = q1.createTailer();

             ChronicleQueue q2 = createQueue(path)) {

            Bytes<?> bytes = Bytes.allocateElasticOnHeap();
            Bytes<?> bytes2 = Bytes.allocateElasticOnHeap();
            for (int cycle = 1; cycle < 10; cycle++) {
                for (int seq = 0; seq < cycle; seq++) {
                    bytes.clear().append("Msg ").append(String.valueOf(cycle)).append(" ").append(String.valueOf(seq));
                    long index = q0.rollCycle().toIndex(cycle, seq);

                    if ((cycle + seq) % 5 < 2) {
                        ((InternalAppender) a0).writeBytes(index, bytes);
                    }

                    // try a1
                    ((InternalAppender) a1).writeBytes(index, bytes);
                    assertTrue(t1.readBytes(bytes2.clear()));
                    if (!bytes.contentEquals(bytes2)) {
                        System.out.println(q2.dump());
                        assertEquals(bytes.toString(), bytes2.toString());
                    }
                    assertFalse(t1.readBytes(bytes2.clear()));

                    assertTrue(t0.readBytes(bytes2.clear()));
                    if (!bytes.contentEquals(bytes2)) {
                        System.out.println(q2.dump());
                        assertEquals(bytes.toString(), bytes2.toString());
                    }
                    assertFalse(t0.readBytes(bytes2.clear()));
                }
            }
        } finally {
            IOTools.deleteDirWithFiles(path);
        }
    }

    static ChronicleQueue createQueue(File path) {
        return SingleChronicleQueueBuilder.single(path)
                .testBlockSize()
                .rollCycle(TEST4_SECONDLY)
                .build();
    }
}
