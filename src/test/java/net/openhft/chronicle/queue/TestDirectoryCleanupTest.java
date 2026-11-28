/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class TestDirectoryCleanupTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void windowsMappingMustBeReleasedBeforeDeletionCanSucceed() throws Exception {
        assumeTrue(OS.isWindows());
        Path owned = folder.newFolder("mapped").toPath();
        MappedBytes bytes = MappedBytes.mappedBytes(owned.resolve("data.cq4").toFile(), 4096);
        Thread releaser = null;
        try {
            bytes.writeLong(1);
            AssertionError failure = assertThrows(AssertionError.class,
                    () -> TestDirectoryCleanup.delete(owned, 30, TestDirectoryCleanup::deleteOnce));
            assertTrue(failure.getCause() instanceof java.nio.file.FileSystemException);
            assertTrue(Files.exists(owned));
            releaser = new Thread(() -> {
                Jvm.pause(100);
                bytes.close();
                BackgroundResourceReleaser.releasePendingResources();
            }, "owned-mapping-release");
            releaser.start();
            TestDirectoryCleanup.delete(owned);
            assertFalse(Files.exists(owned));
        } finally {
            if (releaser != null) {
                releaser.join(5_000);
                assertFalse("Mapping release worker must finish", releaser.isAlive());
            }
            bytes.close();
            BackgroundResourceReleaser.releasePendingResources();
        }
    }

    @Test
    public void retriesTransientSharingFailureAndPreservesUnrelatedFiles() throws Exception {
        Path owned = folder.newFolder("owned").toPath();
        Path unrelated = folder.newFile("unrelated").toPath();
        Files.write(owned.resolve("data.cq4"), new byte[]{1});
        AtomicInteger attempts = new AtomicInteger();
        TestDirectoryCleanup.delete(owned, 2_000, path -> {
            if (attempts.incrementAndGet() == 1)
                throw new AccessDeniedException(path.resolve("data.cq4").toString(), null, "sharing violation");
            TestDirectoryCleanup.deleteOnce(path);
        });
        assertEquals(2, attempts.get());
        assertFalse(Files.exists(owned));
        assertTrue(Files.exists(unrelated));
    }

    @Test
    public void persistentFailureRetainsNativeCauseAndOwnedPath() throws Exception {
        Path owned = folder.newFolder("persistent").toPath();
        AccessDeniedException nativeFailure = new AccessDeniedException(owned.resolve("metadata.cq4t").toString());
        AssertionError failure = assertThrows(AssertionError.class,
                () -> TestDirectoryCleanup.delete(owned, 30, path -> { throw nativeFailure; }));
        assertSame(nativeFailure, failure.getCause());
        assertTrue(failure.getMessage().contains(owned.toString()));
        assertTrue(Files.exists(owned));
    }

    @Test
    public void interruptionStopsCleanupWithoutClearingTheFlag() throws Exception {
        Path owned = folder.newFolder("interrupted").toPath();
        AtomicInteger attempts = new AtomicInteger();
        try {
            Thread.currentThread().interrupt();
            AssertionError failure = assertThrows(AssertionError.class,
                    () -> TestDirectoryCleanup.delete(owned, 2_000, path -> attempts.incrementAndGet()));
            assertTrue(failure.getMessage().contains("interrupted=true"));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(0, attempts.get());
            assertTrue(Files.exists(owned));
        } finally {
            Thread.interrupted();
        }
    }
}
