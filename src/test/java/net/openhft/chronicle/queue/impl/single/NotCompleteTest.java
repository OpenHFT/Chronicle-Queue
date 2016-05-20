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

import net.openhft.chronicle.bytes.ref.BinaryLongReference;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.WireType;
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
}