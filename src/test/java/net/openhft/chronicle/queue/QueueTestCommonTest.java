/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.ExceptionHandler;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.core.onoes.ThreadLocalisedExceptionHandler.unwrap;
import static org.junit.Assert.*;

public class QueueTestCommonTest {
    @Test
    public void failedReferenceCheckStillCleansUpAndResetsHandlers() {
        Fixture fixture = new Fixture();
        fixture.finishedNormally = true;
        File directory = fixture.getTmpDir();
        assertTrue(directory.mkdirs());
        Jvm.recordExceptions(false);
        ExceptionHandler recordingHandler = unwrap(Jvm.warn());
        try {
            assertSame(fixture.failure, assertThrows(AssertionError.class, fixture::afterChecks));
            assertTrue(fixture.cleanedUp);
            assertFalse(directory.exists());
            assertNotSame(recordingHandler, unwrap(Jvm.warn()));
        } finally {
            Jvm.resetExceptionHandlers();
            fixture.tearDown();
        }
    }

    @Test
    public void preAfterFailureStillResetsClockAndPreservesCleanupFailure() {
        Fixture fixture = new Fixture();
        fixture.failBeforeChecks = true;
        fixture.failDuringCleanup = true;
        SystemTimeProvider.CLOCK = new SetTimeProvider(0);
        try {
            assertSame(fixture.failure, assertThrows(AssertionError.class, fixture::afterChecks));
            assertTrue(fixture.cleanedUp);
            assertSame(SystemTimeProvider.INSTANCE, SystemTimeProvider.CLOCK);
            assertArrayEquals(new Throwable[]{fixture.cleanupFailure}, fixture.failure.getSuppressed());
        } finally {
            SystemTimeProvider.CLOCK = SystemTimeProvider.INSTANCE;
            Jvm.resetExceptionHandlers();
        }
    }

    @Test
    public void skippedChecksStillResetHandlers() {
        Fixture fixture = new Fixture();
        fixture.finishedNormally = false;
        Jvm.recordExceptions(false);
        ExceptionHandler recordingHandler = unwrap(Jvm.warn());
        try {
            fixture.afterChecks();
            assertTrue(fixture.cleanedUp);
            assertNotSame(recordingHandler, unwrap(Jvm.warn()));
        } finally {
            Jvm.resetExceptionHandlers();
        }
    }

    private static final class Fixture extends QueueTestCommon {
        private final AssertionError failure = new AssertionError("injected check failure");
        private final IllegalStateException cleanupFailure = new IllegalStateException("injected cleanup failure");
        private boolean cleanedUp;
        private boolean failBeforeChecks;
        private boolean failDuringCleanup;

        @Override
        protected void preAfter() {
            if (failBeforeChecks)
                throw failure;
        }

        @Override
        public void assertReferencesReleased() {
            throw failure;
        }

        @Override
        protected void tearDown() {
            cleanedUp = true;
            super.tearDown();
            if (failDuringCleanup)
                throw cleanupFailure;
        }
    }
}
