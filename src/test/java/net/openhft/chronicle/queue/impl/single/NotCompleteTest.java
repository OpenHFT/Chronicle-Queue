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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ref.BinaryLongArrayReference;
import net.openhft.chronicle.bytes.ref.BinaryLongReference;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static net.openhft.chronicle.queue.ChronicleQueueTestBase.getTmpDir;


public class NotCompleteTest {

    private static final int TIMES = 100;


    /**
     * tests that when flags are set to not complete we are able to recover
     */
    @Test
    public void testUsingANotCompleteQueue() throws TimeoutException, ExecutionException,
            InterruptedException {

        BinaryLongReference.startCollecting();

        File tmpDir = getTmpDir();
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {

            ExcerptAppender appender = queue.createAppender();

            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("some").text("data");
            }

            Thread.sleep(100);

            System.out.println(queue.dump());
        }

        // this is what will corrupt the queue
        BinaryLongReference.forceAllToNotCompleteState();

        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .timeoutMS(500)
                .build()) {
            System.out.println(queue.dump());

            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext documentContext = tailer.readingDocument()) {
                Assert.assertEquals("data", documentContext.wire().read(() -> "some").text());
            }
        }
    }

    @Test
    public void testUsingANotCompleteArrayQueue() throws TimeoutException, ExecutionException,
            InterruptedException {

        BinaryLongArrayReference.startCollecting();

        File tmpDir = getTmpDir();
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .rollCycle(RollCycles.TEST_DAILY)
                .build()) {

            ExcerptAppender appender = queue.createAppender();

            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("some").text("data");
            }

            Thread.sleep(100);

            System.out.println(queue.dump());
        }

        // this is what will corrupt the queue
        BinaryLongArrayReference.forceAllToNotCompleteState();

        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .timeoutMS(500)
                .build()) {
            System.out.println(queue.dump());

            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext documentContext = tailer.readingDocument()) {
                Assert.assertEquals("data", documentContext.wire().read(() -> "some").text());
            }
        }
    }


    @Test
    public void testDocumentLeftInNotRead() throws TimeoutException, ExecutionException,
            InterruptedException {


        File tmpDir = getTmpDir();
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .build()) {

            ExcerptAppender appender = queue.createAppender();


            long start;
            Bytes<?> bytes;

            try (DocumentContext documentContext = appender.writingDocument()) {
                bytes = documentContext.wire().bytes();
                documentContext.wire().write("some").text("data");
            }

            // this should simulate another document being written by another process and left in
            // a  not ready state
            bytes.writeInt(Wires.NOT_READY);
        }


        // this should write it should not time out !

        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .build()) {

            ExcerptAppender appender = queue.createAppender();

            // the above should time out and only the some-more more data should be written
            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("some-more").text("more-data");
            }
        }

        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .build()) {
            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext documentContext = tailer.readingDocument()) {
                Assert.assertEquals("some", documentContext.wire().read(() -> "data").text());
            }


        }


    }


    @Test
    public void testMessageOfUnknownLengthLeftNotComplete() throws TimeoutException, ExecutionException,
            InterruptedException {


        File tmpDir = getTmpDir();
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .build()) {

            ExcerptAppender appender = queue.createAppender();


            Bytes<?> bytes;

            try (DocumentContext documentContext = appender.writingDocument()) {
                bytes = documentContext.wire().bytes();
                documentContext.wire().write("some").text("data");
            }

            // this should simulate another document being written by another process and left in
            // a  not ready state
            bytes.writeInt(Wires.NOT_READY);

            System.out.println(queue.dump().toString());

        }


        // this should write it should not time out !

        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .build()) {

            ExcerptAppender appender = queue.createAppender();

            // the above should time out and only the some-more more data should be written
            try (DocumentContext documentContext = appender.writingDocument()) {
                documentContext.wire().write("some-more").text("more-data");
            }


        }

        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .build()) {
            ExcerptTailer tailer = queue.createTailer();

            try (DocumentContext documentContext = tailer.readingDocument()) {
                Assert.assertEquals("some", documentContext.wire().read(() -> "data").text());
            }


        }


    }


    @Test
    public void testMessageOfKnownLengthLeftNotComplete() throws TimeoutException,
            ExecutionException,
            InterruptedException {


        File tmpDir = getTmpDir();
        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .build()) {

            ExcerptAppender appender = queue.createAppender();


            long start;
            Bytes<?> bytes;

            try (DocumentContext documentContext = appender.writingDocument()) {
                bytes = documentContext.wire().bytes();
                start = bytes.writePosition() - 4;
                documentContext.wire().write("some").text("data");
            }

            // leave the document in a not read state - not sure about the index ?
            bytes.writePosition(start).writeInt(bytes.readInt(start) | Wires.NOT_READY);


            System.out.println(queue.dump().toString());
        }


        try (final RollingChronicleQueue queue = new SingleChronicleQueueBuilder(tmpDir)
                .wireType(WireType.BINARY)
                .build()) {
            ExcerptTailer tailer = queue.createTailer().lazyIndexing(true);

            try (DocumentContext documentContext = tailer.readingDocument()) {
                Assert.assertEquals("some", documentContext.wire().read(() -> "data").text());
            }


        }


    }
}