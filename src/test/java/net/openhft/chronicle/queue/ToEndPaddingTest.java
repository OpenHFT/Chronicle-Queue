/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST8_DAILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ToEndPaddingTest extends QueueTestCommon {
    @Test
    public void toEndWorksWithDifferentlyPaddedMessages() {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(getTmpDir()).testBlockSize().rollCycle(TEST8_DAILY).build();
             final ExcerptAppender appender = queue.createAppender()) {

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
                    documentContext.wire().write("metakey" + i).text(Bytes.wrapForRead(new byte[i + 1]));
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
