/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class QueueTestCommonAfterChecksTest {
    @Test
    public void earlyFailureStillRestoresStateAndDeletesOwnedDirectories() throws Exception {
        AssertionError early = new AssertionError("early check");
        AssertionError later = new AssertionError("resource check");
        QueueTestCommon fixture = new QueueTestCommon() {
            @Override
            protected void preAfter() {
                throw early;
            }

            @Override
            public void assertReferencesReleased() {
                throw later;
            }
        };
        File dir = fixture.getTmpDir();
        Files.createDirectories(dir.toPath());
        Files.createFile(dir.toPath().resolve("data"));
        fixture.assumeFinishedNormally();
        fixture.recordExceptions();
        SystemTimeProvider.CLOCK = new SetTimeProvider(1);
        try {
            AssertionError failure = assertThrows(AssertionError.class, fixture::afterChecks);
            assertSame(early, failure);
            assertArrayEquals(new Throwable[]{later}, failure.getSuppressed());
            assertSame(SystemTimeProvider.INSTANCE, SystemTimeProvider.CLOCK);
            assertFalse("An early check must not bypass directory cleanup", dir.exists());
        } finally {
            SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;
            Jvm.resetExceptionHandlers();
            IOTools.deleteDirWithFiles(dir);
        }
    }

    @Test
    public void failedDirectoryDoesNotHideTheOriginalFailureOrSkipTheNextDirectory() throws Exception {
        AssertionError early = new AssertionError("early check");
        QueueTestCommon fixture = new QueueTestCommon() {
            @Override
            protected void preAfter() {
                throw early;
            }
        };
        File tooDeep = fixture.getTmpDir();
        File later = fixture.getTmpDir();
        java.nio.file.Path nested = tooDeep.toPath();
        for (int i = 0; i < 22; i++)
            nested = nested.resolve("d");
        Files.createDirectories(nested);
        Files.createDirectories(later.toPath());
        try {
            AssertionError failure = assertThrows(AssertionError.class, fixture::afterChecks);
            assertSame(early, failure);
            assertEquals(1, failure.getSuppressed().length);
            assertTrue(failure.getSuppressed()[0] instanceof AssertionError);
            assertTrue(failure.getSuppressed()[0].getMessage().contains(tooDeep.getPath()));
            assertTrue("The fallback must retain its original depth limit", tooDeep.exists());
            assertFalse("Cleanup must continue with later owned directories", later.exists());
        } finally {
            IOTools.deleteDirWithFiles(tooDeep, 30);
            IOTools.deleteDirWithFiles(later);
        }
    }
}
