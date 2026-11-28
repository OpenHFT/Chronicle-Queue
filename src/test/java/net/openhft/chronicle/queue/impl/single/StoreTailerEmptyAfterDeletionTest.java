/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class StoreTailerEmptyAfterDeletionTest extends QueueTestCommon {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void toStartFromStartAfterOfflineQueueDeletion() throws IOException {
        verifyBothQueueModes(Position.START, Position.START);
    }

    @Test
    public void toStartFromEndAfterOfflineQueueDeletion() throws IOException {
        verifyBothQueueModes(Position.END, Position.START);
    }

    @Test
    public void toEndFromEndAfterOfflineQueueDeletion() throws IOException {
        verifyBothQueueModes(Position.END, Position.END);
    }

    @Test
    public void toEndFromStartAfterOfflineQueueDeletion() throws IOException {
        verifyBothQueueModes(Position.START, Position.END);
    }

    private void verifyBothQueueModes(Position initialPosition, Position operation) throws IOException {
        assumeFalse(OS.isWindows());
        verifyEmptyRecovery(initialPosition, operation, false);
        verifyEmptyRecovery(initialPosition, operation, true);
    }

    private void verifyEmptyRecovery(Position initialPosition, Position operation,
                                     boolean readOnly) throws IOException {
        final SetTimeProvider time = new SetTimeProvider(System.currentTimeMillis());
        final File directory = temporaryFolder.newFolder(readOnly ? "read-only" : "writable");
        try (SingleChronicleQueue writerQueue = newQueue(directory, false, time);
             ExcerptAppender appender = writerQueue.createAppender();
             ExcerptTailer tailer = writerQueue.createTailer()) {
            appender.writeText("only-entry");
            initialPosition.move(tailer);
            assertEquals(initialPosition == Position.START ? appender.lastIndexAppended() : appender.lastIndexAppended() + 1,
                    tailer.index());
        }

        //! Integration control for QUEUE-146: deleting the current roll while its published metadata survives is not an empty queue.
        //! Close all handles and delete the complete queue before testing empty positioning and subsequent appends in either mode.
        //! Historical deletion with a live tailer is covered separately by ReadOnlyToStartAfterDeletionTest and TestDeleteQueueFile.
        BackgroundResourceReleaser.releasePendingResources();
        assertTrue("offline deletion includes the published metadata", IOTools.deleteDirWithFiles(directory));
        Files.createDirectory(directory.toPath());
        try (SingleChronicleQueue emptyQueue = newQueue(directory, false, time)) {
            assertEquals("initialise empty metadata before opening a read-only queue", 0, emptyQueue.entryCount());
        }
        BackgroundResourceReleaser.releasePendingResources();

        try (SingleChronicleQueue queue = newQueue(directory, readOnly, time);
             ExcerptTailer tailer = queue.createTailer()) {
            initialPosition.move(tailer);

            for (int attempt = 0; attempt < 3; attempt++) {
                operation.move(tailer);
                assertEquals("public empty-queue index", 0, tailer.index());
                assertEquals("cycle", Integer.MIN_VALUE, tailer.cycle());
                assertEquals("state", TailerState.UNINITIALISED, tailer.state());
                assertNull("the reopened empty queue must not retain a store", tailer.currentFile());
                try (DocumentContext document = tailer.readingDocument()) {
                    assertFalse("empty tailer must not expose a document", document.isPresent());
                }
                assertFalse("a missing position must remain unavailable", tailer.moveToIndex(0));
            }

            time.advanceMillis(RollCycles.FAST_DAILY.lengthInMillis());
            final long nextIndex;
            try (SingleChronicleQueue writerQueue = newQueue(directory, false, time);
                 ExcerptAppender appender = writerQueue.createAppender()) {
                appender.writeText("next-entry");
                nextIndex = appender.lastIndexAppended();
            }
            queue.refreshDirectoryListing();
            operation.move(tailer);
            assertEquals("position in the surviving later cycle",
                    operation == Position.START ? nextIndex : nextIndex + 1, tailer.index());
            if (operation == Position.END) {
                try (DocumentContext document = tailer.readingDocument()) {
                    assertFalse("toEnd must remain after the later message", document.isPresent());
                }
                tailer.toStart();
            }
            try (DocumentContext document = tailer.readingDocument()) {
                assertTrue("the surviving later message must be readable", document.isPresent());
                assertEquals(nextIndex, document.index());
                assertEquals("next-entry", document.wire().getValueIn().text());
            }
        }
    }

    private static SingleChronicleQueue newQueue(File directory, boolean readOnly, SetTimeProvider time) {
        return SingleChronicleQueueBuilder.binary(directory)
                .rollCycle(RollCycles.FAST_DAILY)
                .timeProvider(time)
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

}
