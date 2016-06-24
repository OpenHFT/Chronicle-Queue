/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.bytes.ref.BinaryLongArrayReference;
import net.openhft.chronicle.bytes.ref.BinaryLongReference;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static net.openhft.chronicle.queue.ChronicleQueueTestBase.getTmpDir;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class NotCompleteTest {

    /**
     * tests that when flags are set to not complete we are able to recover
     */
    @Test
    public void testUsingANotCompleteQueue()
            throws TimeoutException, ExecutionException, InterruptedException {

        BinaryLongReference.startCollecting();

        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = binary(tmpDir)
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("some").text("data");
            }

            Thread.sleep(100);

//            System.out.println(queue.dump());
        }

        // this is what will corrupt the queue
        BinaryLongReference.forceAllToNotCompleteState();

        try (final ChronicleQueue queue = binary(tmpDir)
                .timeoutMS(500)
                .build()) {
//            System.out.println(queue.dump());

            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("data", dc.wire().read(() -> "some").text());
            }
        }
    }

    @Test
    public void testUsingANotCompleteArrayQueue()
            throws TimeoutException, ExecutionException, InterruptedException {

        BinaryLongArrayReference.startCollecting();

        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = binary(tmpDir)
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {

            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("some").text("data");
            }

            Thread.sleep(100);

//            System.out.println(queue.dump());
        }

        // this is what will corrupt the queue
        BinaryLongArrayReference.forceAllToNotCompleteState();

        try (final ChronicleQueue queue = binary(tmpDir)
                .timeoutMS(500)
                .build()) {
//            System.out.println(queue.dump());

            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("data", dc.wire().read(() -> "some").text());
            }
        }
    }

    @Test
    public void testMessageLeftNotComplete()
            throws TimeoutException, ExecutionException, InterruptedException {

        File tmpDir = getTmpDir();
        try (final ChronicleQueue queue = binary(tmpDir).rollCycle(RollCycles.TEST_DAILY).build()) {
            ExcerptAppender appender = queue.acquireAppender();

            // start a message which was not completed.
            DocumentContext dc = appender.writingDocument();
            dc.wire().write("some").text("data");
            // didn't call dc.close();

//            System.out.println(queue.dump());
        }

        try (final ChronicleQueue queue = binary(tmpDir).build()) {
            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertFalse(dc.isPresent());
            }
        }

        try (final ChronicleQueue queue = binary(tmpDir).timeoutMS(500).build()) {
            ExcerptAppender appender = queue.acquireAppender();

            try (DocumentContext dc = appender.writingDocument()) {
                dc.wire().write("some").text("data");
            }

//            System.out.println(queue.dump());
        }

        try (final ChronicleQueue queue = binary(tmpDir).build()) {
            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext dc = tailer.readingDocument()) {
                assertEquals("data", dc.wire().read(() -> "some").text());
            }
        }
    }
}