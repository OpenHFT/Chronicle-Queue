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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueExcerpts.InternalAppender;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import static net.openhft.chronicle.queue.RollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Rob Austin.
 */
public class CreateAtIndexTest {

    @Test
    public void testWriteBytesWithIndex() throws Exception {
        String tmp = OS.TARGET + "/CreateAtIndexTest-" + System.nanoTime();
        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp)
                .rollCycle(TEST_DAILY).build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            appender.writeBytes(0x421d00000000L, Bytes.from("hello world"));
            appender.writeBytes(0x421d00000001L, Bytes.from("hello world"));
        }
        // try again and fail.
        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

//            try {
                appender.writeBytes(0x421d00000000L, Bytes.from("hello world"));
//                fail();
//            } catch (IllegalStateException e) {
//                assertEquals("Unable to move to index 421d00000000 as the index already exists",
//                        e.getMessage());
//            }
        }

        // try too far
        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            try {
                appender.writeBytes(0x421d00000003L, Bytes.from("hello world"));
                fail();
            } catch (IllegalStateException e) {
                assertEquals("Unable to move to index 421d00000003 beyond the end of the queue",
                        e.getMessage());
            }
        }

        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {
            InternalAppender appender = (InternalAppender) queue.acquireAppender();

            appender.writeBytes(0x421d00000002L, Bytes.from("hello world"));
            appender.writeBytes(0x421d00000003L, Bytes.from("hello world"));
        }

        try {
            IOTools.deleteDirWithFiles(tmp, 2);
        } catch (IORuntimeException ignored) {
        }
    }

    @Test
    public void testWrittenAndReadIndexesAreTheSameOfTheFirstExcerpt() throws Exception {
        String tmp = OS.TARGET + "/CreateAtIndexTest-" + System.nanoTime();

        long expected = 0;

        try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(tmp).build()) {

            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {

                dc.wire().write().text("some-data");

                expected = dc.index();
                Assert.assertTrue(expected > 0);

            }

            appender.lastIndexAppended();

            ExcerptTailer tailer = queue.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {

                String text = dc.wire().read().text();

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