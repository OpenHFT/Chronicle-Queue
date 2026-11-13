/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.io.BackgroundResourceReleaser;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;

/** Disposable, synchronous work for the lazy initialization holder in {@link ToolsUtil}. */
final class IndexWarmup {
    private IndexWarmup() {
    }

    static RuntimeException run() {
        Path directory = null;
        RuntimeException failure = null;
        try {
            directory = Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")),
                    "chronicle-queue-index-warmup-");
            // Fixed time prevents a wall-clock roll during warm-up. Four index arrays are exercised.
            exerciseIndex(directory);
        } catch (IOException | RuntimeException e) {
            failure = new IllegalStateException("Unable to warm Queue indexing", e);
        } finally {
            if (directory != null) {
                try {
                    // Closing the queue can enqueue mapped-file releases. Windows cannot
                    // delete those files until the releases have completed.
                    BackgroundResourceReleaser.releasePendingResources();
                    deleteDirectory(directory);
                } catch (IOException | RuntimeException e) {
                    failure = cleanupFailure(directory, failure, e);
                }
            }
        }
        return failure;
    }

    private static RuntimeException cleanupFailure(Path directory, RuntimeException primary, Exception cleanup) {
        if (primary == null)
            return new IllegalStateException("Unable to delete Queue warm-up files at " + directory, cleanup);
        primary.addSuppressed(cleanup);
        return primary;
    }

    private static void exerciseIndex(Path directory) {
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(directory.toFile())
                .rollCycle(RollCycles.FAST_DAILY).timeProvider(() -> 0L)
                .indexSpacing(4).indexCount(4096).blockSize(4L << 20).build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailer = queue.createTailer()) {
            for (int i = 0; i < 64_000; i++) {
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().bytes().writeInt(i);
                }
                if ((i & 3) == 0 && !tailer.moveToIndex(appender.lastIndexAppended()))
                    throw new IllegalStateException("Unable to seek warm-up excerpt " + i);
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent() || dc.wire().bytes().readInt() != i)
                        throw new IllegalStateException("Unable to read warm-up excerpt " + i);
                }
            }
        }
    }

    private static void deleteDirectory(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exception) throws IOException {
                if (exception != null)
                    throw exception;
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

}
