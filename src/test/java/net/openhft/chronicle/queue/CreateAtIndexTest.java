/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.IllegalIndexException;
import net.openhft.chronicle.queue.impl.single.InternalAppender;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RequiredForClient
public class CreateAtIndexTest extends QueueTestCommon {

    @Test
    public void
    testWriteBytesWithIndex() {
        final Bytes<?> HELLO_WORLD = Bytes.from("hello world");
        File tmp = getTmpDir();
        try (ChronicleQueue queue = single(tmp).testBlockSize().rollCycle(TEST_DAILY).build();
             InternalAppender appender = (InternalAppender) queue.createAppender()) {

            appender.writeBytes(0x421d00000000L, HELLO_WORLD);
            appender.writeBytes(0x421d00000001L, HELLO_WORLD);
        }

        try (ChronicleQueue queue = single(tmp)
                .testBlockSize()
                .build();
             InternalAppender appender = (InternalAppender) queue.createAppender()) {

            String before = queue.dump();
            appender.writeBytes(0x421d00000000L, HELLO_WORLD);
            String after = queue.dump();
            assertEquals(before, after);
        }

/*
        TODO FIX
        if (Jvm.isAssertEnabled()) {
            try (ChronicleQueue queue = single(tmp)
                    .testBlockSize()
                    .build()) {
                InternalAppender appender = (InternalAppender) queue.acquireAppender();

                String before = queue.dump();
                try {
                    appender.writeBytes(0x421d00000000L, Bytes.from("hellooooo world"));
                    fail();
                } catch (IllegalStateException e) {
                    // expected
                }
                String after = queue.dump();
                assertEquals(before, after);
            }
        }
        */

        // try too far
        try (ChronicleQueue queue = single(tmp)
                .testBlockSize()
                .build();
             InternalAppender appender = (InternalAppender) queue.createAppender()) {

            IllegalIndexException e = assertThrows(IllegalIndexException.class, () -> appender.writeBytes(0x421d00000003L, HELLO_WORLD));
            assertEquals("Index provided is after the next index in the queue, provided index = 421d00000003, last index in queue = 421d00000001", e.getMessage());
        }

        try (ChronicleQueue queue = single(tmp)
                .testBlockSize()
                .build();
             InternalAppender appender = (InternalAppender) queue.createAppender()) {

            appender.writeBytes(0x421d00000002L, HELLO_WORLD);
            appender.writeBytes(0x421d00000003L, HELLO_WORLD);
        }

        try {
            IOTools.deleteDirWithFiles(tmp, 2);
        } catch (IORuntimeException ignored) {
        }
    }

    @Test
    public void testWrittenAndReadIndexesAreTheSameOfTheFirstExcerpt() {
        File tmp = getTmpDir();

        long expected;

        try (ChronicleQueue queue = single(tmp)
                .testBlockSize()
                .build();
             ExcerptAppender appender = queue.createAppender()) {

            try (DocumentContext dc = appender.writingDocument()) {

                dc.wire().write().text("some-data");

                expected = dc.index();
                Assert.assertTrue(expected > 0);

            }

            appender.lastIndexAppended();

            ExcerptTailer tailer = queue.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {

                dc.wire().read().text();

                {
                    long actualIndex = dc.index();
                    Assert.assertTrue(actualIndex > 0);

                    Assert.assertEquals(expected, actualIndex);
                }

                {
                    long actualIndex = tailer.index();
                    Assert.assertTrue(actualIndex > 0);

                    Assert.assertEquals(expected, actualIndex);
                }
            }
        }
    }
}
