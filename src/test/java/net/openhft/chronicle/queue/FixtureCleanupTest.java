/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@SuppressWarnings("try")
public class FixtureCleanupTest {
    @Test
    public void retainsBodyFailureAndAttemptsEveryCleanup() {
        AssertionError body = new AssertionError("body");
        AssertionError first = new AssertionError("first cleanup");
        IllegalStateException second = new IllegalStateException("second cleanup");
        AtomicBoolean lastAttempted = new AtomicBoolean();
        Throwable actual = assertThrows(AssertionError.class, () -> {
            try (FixtureCleanup ignored = new FixtureCleanup(
                    () -> { throw first; }, () -> { throw second; }, () -> lastAttempted.set(true))) {
                throw body;
            }
        });
        assertSame(body, actual);
        assertArrayEquals(new Throwable[]{first}, body.getSuppressed());
        assertArrayEquals(new Throwable[]{second}, first.getSuppressed());
        assertTrue(lastAttempted.get());
    }

    @Test
    public void cleanupFailureStillFailsASuccessfulBody() {
        AssertionError cleanup = new AssertionError("cleanup");
        Throwable actual = assertThrows(AssertionError.class, () -> {
            try (FixtureCleanup ignored = new FixtureCleanup(() -> { throw cleanup; })) {
                // Successful body.
            }
        });
        assertSame(cleanup, actual);
    }
}
