/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST8_DAILY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ToEndPaddingTest extends QueueTestCommon {
    @Test
    @DisplayName("toEnd handles padding across variable length messages")
    public void toEndWorksWithDifferentlyPaddedMessages() {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(getTmpDir()).testBlockSize().rollCycle(TEST8_DAILY).build();
             final ExcerptAppender appender = queue.createAppender()) {

            final ExcerptTailer tailer = queue.createTailer();

            try (final DocumentContext documentContext = appender.acquireWritingDocument(false)) {
                documentContext.wire().write("start").text("start");
            }

            DocumentContext dc;
            try (final DocumentContext documentContext = tailer.readingDocument(false)) {
                assertTrue(documentContext.isPresent(), "document context should be present for initial read");

                final String text = documentContext.wire().read().text();

                assertEquals("start", text, "read message text should match written start content");

                // cache for later
                dc = documentContext;
            }

            for (int i = 0; i < 2; i++) {
                try (final DocumentContext documentContext = appender.acquireWritingDocument(true)) {
                    documentContext.wire().write("metakey" + i).text(Bytes.wrapForRead(new byte[i + 1]));
                }
            }

            // toEnd just before adding one more entry
            assertEquals(2336, dc.wire().bytes().readPosition(),
                    "read position before toEnd padding should be 2336");
            tailer.toEnd();
            assertEquals(2368, dc.wire().bytes().readPosition(),
                    "read position after toEnd padding should be 2368");

            try (final DocumentContext documentContext = appender.acquireWritingDocument(false)) {
                documentContext.wire().write("key").text("value");
            }

            try (final DocumentContext documentContext = tailer.readingDocument(false)) {
                assertTrue(documentContext.isPresent(), "document context should be present after toEnd read");

                final String text = documentContext.wire().read().text();

                assertEquals("value", text, "read message text should match written value content after toEnd");
            }
        }
    }
}
