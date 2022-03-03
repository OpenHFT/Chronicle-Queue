/*
 * Copyright 2016-2020 Chronicle Software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ToEndPaddingTest extends ChronicleQueueTestBase {
    @Test
    public void toEndWorksWithDifferentlyPaddedMessages() {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(getTmpDir()).testBlockSize().rollCycle(RollCycles.TEST8_DAILY).build()) {
            final ExcerptAppender appender = queue.acquireAppender();

            final ExcerptTailer tailer = queue.createTailer();

            try (final DocumentContext documentContext = appender.acquireWritingDocument(false)) {
                documentContext.wire().write("start").text("start");
            }

            DocumentContext dc;
            try (final DocumentContext documentContext = tailer.readingDocument(false)) {
                assertTrue(documentContext.isPresent());

                final String text = documentContext.wire().read().text();

                assertEquals("start", text);

                // cache for later
                dc = documentContext;
            }

            for (int i = 0; i < 2; i++) {
                try (final DocumentContext documentContext = appender.acquireWritingDocument(true)) {
                    documentContext.wire().write("metakey" + i).text(Bytes.wrapForRead(new byte[i+1]));
                }
            }

//            System.out.println(queue.dump());

            // toEnd just before adding one more entry
            assertEquals(2336, dc.wire().bytes().readPosition());
            tailer.toEnd();
            assertEquals(2368, dc.wire().bytes().readPosition());

            try (final DocumentContext documentContext = appender.acquireWritingDocument(false)) {
                documentContext.wire().write("key").text("value");
            }

            try (final DocumentContext documentContext = tailer.readingDocument(false)) {
                assertTrue(documentContext.isPresent());

                final String text = documentContext.wire().read().text();

                assertEquals("value", text);
            }
        }
    }
}
