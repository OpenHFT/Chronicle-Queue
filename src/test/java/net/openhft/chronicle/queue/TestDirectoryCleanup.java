/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

/** Deletes only the fixture passed by its owner, after its resources have been closed. */
final class TestDirectoryCleanup {
    private TestDirectoryCleanup() {
    }

    static void delete(Path directory) {
        BackgroundResourceReleaser.releasePendingResources();
        delete(directory, 2_000, TestDirectoryCleanup::deleteOnce);
    }

    static void delete(Path directory, long timeoutMillis, Deletion deletion) {
        long start = System.nanoTime();
        IOException first = null;
        IOException last = null;
        int attempts = 0;
        do {
            if (Thread.currentThread().isInterrupted())
                break;
            try {
                attempts++;
                deletion.delete(directory);
                return;
            } catch (IOException failure) {
                if (first == null)
                    first = failure;
                last = failure;
            }
            if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= timeoutMillis)
                break;
            // Windows can retain a sharing restriction briefly after the owner exits.
            Jvm.pause(10);
        } while (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < timeoutMillis);

        AssertionError failure = new AssertionError("Could not delete owned test directory " + directory
                + "; attempts=" + attempts
                + ", elapsedMillis=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
                + ", interrupted=" + Thread.currentThread().isInterrupted()
                + ", lastFailure=" + last);
        if (first != null)
            failure.initCause(first);
        if (last != null && last != first)
            failure.addSuppressed(last);
        throw failure;
    }

    static void deleteOnce(Path directory) throws IOException {
        if (Files.notExists(directory, LinkOption.NOFOLLOW_LINKS))
            return;
        // Do not follow links out of the owned fixture.
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException failure) throws IOException {
                if (failure != null)
                    throw failure;
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @FunctionalInterface
    interface Deletion {
        void delete(Path directory) throws IOException;
    }
}
