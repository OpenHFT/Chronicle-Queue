/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.TailerState;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeFalse;

public class StoreTailerEmptyAfterDeletionTest extends QueueTestCommon {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void toStartResetsWhenAlreadyAtDeletedSoleStart() throws IOException {
        verifyBothQueueModes(Position.START, Position.START);
    }

    @Test
    public void toStartResetsWhenElsewhereAfterDeletingSoleRoll() throws IOException {
        verifyBothQueueModes(Position.END, Position.START);
    }

    @Test
    public void toEndResetsWhenAlreadyAtDeletedSoleEnd() throws IOException {
        verifyBothQueueModes(Position.END, Position.END);
    }

    @Test
    public void toEndResetsWhenElsewhereAfterDeletingSoleRoll() throws IOException {
        verifyBothQueueModes(Position.START, Position.END);
    }

    private void verifyBothQueueModes(Position initialPosition, Position operation) throws IOException {
        assumeFalse(OS.isWindows());
        verifyEmptyRecovery(initialPosition, operation, false);
        verifyEmptyRecovery(initialPosition, operation, true);
    }

    private void verifyEmptyRecovery(Position initialPosition, Position operation,
                                     boolean readOnly) throws IOException {
        final TailerPosition emptyPosition = freshEmptyPosition(
                temporaryFolder.newFolder(readOnly ? "empty-read-only-baseline" : "empty-writable-baseline"));
        final File directory = temporaryFolder.newFolder(readOnly ? "read-only" : "writable");
        try (SingleChronicleQueue writerQueue = newQueue(directory, false);
             ExcerptAppender appender = writerQueue.createAppender()) {
            appender.writeText("only-entry");
        }

        final Path rollFile;
        try (Stream<Path> files = Files.list(directory.toPath())) {
            rollFile = files.filter(path -> path.toString().endsWith(SingleChronicleQueue.SUFFIX))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("queue roll file not found"));
        }

        try (SingleChronicleQueue queue = newQueue(directory, readOnly);
             ExcerptTailer tailer = queue.createTailer()) {
            initialPosition.move(tailer);
            Files.delete(rollFile);

            operation.move(tailer);

            assertEquals("index", emptyPosition.index, tailer.index());
            assertEquals("cycle", emptyPosition.cycle, tailer.cycle());
            assertEquals("state", emptyPosition.state, tailer.state());
            assertNull("deleted store must be released", tailer.currentFile());
            try (DocumentContext document = tailer.readingDocument()) {
                assertFalse("empty tailer must not expose a document", document.isPresent());
            }
        }
    }

    private static TailerPosition freshEmptyPosition(File directory) {
        try (SingleChronicleQueue queue = newQueue(directory, false);
             ExcerptTailer tailer = queue.createTailer()) {
            return new TailerPosition(tailer.index(), tailer.cycle(), tailer.state());
        }
    }

    private static SingleChronicleQueue newQueue(File directory, boolean readOnly) {
        return SingleChronicleQueueBuilder.binary(directory)
                .rollCycle(RollCycles.FAST_DAILY)
                .testBlockSize()
                .readOnly(readOnly)
                .build();
    }

    private enum Position {
        START {
            @Override
            void move(ExcerptTailer tailer) {
                tailer.toStart();
            }
        },
        END {
            @Override
            void move(ExcerptTailer tailer) {
                tailer.toEnd();
            }
        };

        abstract void move(ExcerptTailer tailer);
    }

    private static final class TailerPosition {
        final long index;
        final int cycle;
        final TailerState state;

        TailerPosition(long index, int cycle, TailerState state) {
            this.index = index;
            this.cycle = cycle;
            this.state = state;
        }
    }
}
