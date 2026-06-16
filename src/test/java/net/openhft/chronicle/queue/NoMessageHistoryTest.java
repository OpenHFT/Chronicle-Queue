/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NoMessageHistoryTest extends QueueTestCommon {

    @Test
    void testSingletonInstance() {
        // Test that the singleton instance is available and not null
        assertNotNull(NoMessageHistory.INSTANCE);
    }

    @Test
    void testTimings() {
        // Test that timings() always returns 0
        assertEquals(0, NoMessageHistory.INSTANCE.timings());
    }

    @Test
    void testTimingForIndex() {
        // Test that timing(int n) always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.timing(0));
        assertEquals(-1, NoMessageHistory.INSTANCE.timing(1));
    }

    @Test
    void testSources() {
        // Test that sources() always returns 0
        assertEquals(0, NoMessageHistory.INSTANCE.sources());
    }

    @Test
    void testSourceIdForIndex() {
        // Test that sourceId(int n) always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceId(0));
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceId(1));
    }

    @Test
    void testSourceIdsEndsWith() {
        // Test that sourceIdsEndsWith(int[] sourceIds) always returns false
        assertFalse(NoMessageHistory.INSTANCE.sourceIdsEndsWith(new int[]{1, 2, 3}));
    }

    @Test
    void testSourceIndexForIndex() {
        // Test that sourceIndex(int n) always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceIndex(0));
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceIndex(1));
    }

    @Test
    void testResetWithParameters() {
        // Test that reset(int sourceId, long sourceIndex) performs no action (no exceptions thrown)
        NoMessageHistory.INSTANCE.reset(1, 100L);
        assertTrue(true); // if we got here without an exception, the test passes
    }

    @Test
    void testResetWithoutParameters() {
        // Test that reset() performs no action (no exceptions thrown)
        NoMessageHistory.INSTANCE.reset();
        assertTrue(true); // if we got here without an exception, the test passes
    }

    @Test
    void testLastSourceId() {
        // Test that lastSourceId() always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.lastSourceId());
    }

    @Test
    void testLastSourceIndex() {
        // Test that lastSourceIndex() always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.lastSourceIndex());
    }

    @Test
    void testIsDirty() {
        // Test that isDirty() always returns false
        assertFalse(NoMessageHistory.INSTANCE.isDirty());
    }
}
