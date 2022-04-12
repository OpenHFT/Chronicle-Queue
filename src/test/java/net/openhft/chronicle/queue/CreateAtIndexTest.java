/*
 * Copyright 2016-2020 chronicle.software
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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.InternalAppender;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.queue.RollCycles.TEST_DAILY;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.*;

@RequiredForClient
public class CreateAtIndexTest extends ChronicleQueueTestBase {

    @Test
    public void
    testWriteBytesWithIndex() {
        final Bytes<?> HELLO_WORLD = Bytes.from("hello world");
        File tmp = getTmpDir();
        try (ChronicleQueue queue = single(tmp).testBlockSize().rollCycle(TEST_DAILY).build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            appender.writeBytes(0x421d00000000L, HELLO_WORLD);
            appender.writeBytes(0x421d00000001L, HELLO_WORLD);
        }

        try (ChronicleQueue queue = single(tmp)
                .testBlockSize()
                .build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

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
                .build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            try {
                appender.writeBytes(0x421d00000003L, HELLO_WORLD);
                fail();
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().startsWith("Unable to move to index 421d00000003 beyond the end of the queue"));
            }
        }

        try (ChronicleQueue queue = single(tmp)
                .testBlockSize()
                .build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            appender.writeBytes(0x421d00000002L, HELLO_WORLD);
            appender.writeBytes(0x421d00000003L, HELLO_WORLD);
        }

        try {
            IOTools.deleteDirWithFiles(tmp, 2);
        } catch (IORuntimeException ignored) {
        }
    }

    // TODO: 2 or more threads soak test

    @Test
    public void testWrittenAndReadIndexesAreTheSameOfTheFirstExcerpt() {
        File tmp = getTmpDir();

        long expected;

        try (ChronicleQueue queue = single(tmp)
                .testBlockSize()
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();

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
