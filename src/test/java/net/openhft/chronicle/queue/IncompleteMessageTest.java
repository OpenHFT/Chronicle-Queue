/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class IncompleteMessageTest extends QueueTestCommon {
    @TempDir
    Path tmpDir;

    @Test
    @DisplayName("Incomplete message is skipped after reopen")
    public void incompleteMessageShouldBeSkipped() {
        System.setProperty("queue.force.unlock.mode", "ALWAYS");
        expectException("Couldn't acquire write lock after ");
        expectException("Forced unlock for the lock ");
        ignoreException("Unable to release the lock");
        try (SingleChronicleQueue queue = createQueue();
             ExcerptAppender appender = queue.createAppender()) {
            appender.writeDocument("hello", ValueOut::text);

            // open a document context, but do not close
            final DocumentContext documentContext = appender.writingDocument();
            documentContext.wire().bytes().write("incomplete longer write".getBytes(StandardCharsets.UTF_8));
        }

        try (SingleChronicleQueue queue = createQueue()) {
            try (ExcerptAppender appender = queue.createAppender()) {
                appender.writeDocument("world", ValueOut::text);
            }

            try (ExcerptTailer tailer = queue.createTailer()) {
                tailer.toStart();
                assertEquals("hello", tailer.readText(), "Tailer should read 'hello' as first complete message, skipping incomplete message");
                assertEquals("world", tailer.readText(), "Tailer should read 'world' as second complete message after reopening queue");
                assertFalse(tailer.readingDocument().isPresent(), "Tailer should find no more complete documents after reading all valid messages");
            }
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
    }

    private SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.binary(tmpDir.toFile()).timeoutMS(250).build();
    }
}
