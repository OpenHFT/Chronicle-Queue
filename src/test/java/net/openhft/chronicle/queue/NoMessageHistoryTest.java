/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;

import static org.junit.Assert.*;

public class NoMessageHistoryTest extends QueueTestCommon {

    @Test
    public void testSingletonInstance() {
        // Test that the singleton instance is available and not null
        assertNotNull(NoMessageHistory.INSTANCE);
    }

    @Test
    public void testTimings() {
        // Test that timings() always returns 0
        assertEquals(0, NoMessageHistory.INSTANCE.timings());
    }

    @Test
    public void testTimingForIndex() {
        // Test that timing(int n) always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.timing(0));
        assertEquals(-1, NoMessageHistory.INSTANCE.timing(1));
    }

    @Test
    public void testSources() {
        // Test that sources() always returns 0
        assertEquals(0, NoMessageHistory.INSTANCE.sources());
    }

    @Test
    public void testSourceIdForIndex() {
        // Test that sourceId(int n) always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceId(0));
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceId(1));
    }

    @Test
    public void testSourceIdsEndsWith() {
        // Test that sourceIdsEndsWith(int[] sourceIds) always returns false
        assertFalse(NoMessageHistory.INSTANCE.sourceIdsEndsWith(new int[] {1, 2, 3}));
    }

    @Test
    public void testSourceIndexForIndex() {
        // Test that sourceIndex(int n) always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceIndex(0));
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceIndex(1));
    }

    @Test
    public void testResetWithParameters() {
        // Test that reset(int sourceId, long sourceIndex) performs no action (no exceptions thrown)
        NoMessageHistory.INSTANCE.reset(1, 100L);
        assertTrue(true); // if we got here without an exception, the test passes
    }

    @Test
    public void testResetWithoutParameters() {
        // Test that reset() performs no action (no exceptions thrown)
        NoMessageHistory.INSTANCE.reset();
        assertTrue(true); // if we got here without an exception, the test passes
    }

    @Test
    public void testLastSourceId() {
        // Test that lastSourceId() always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.lastSourceId());
    }

    @Test
    public void testLastSourceIndex() {
        // Test that lastSourceIndex() always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.lastSourceIndex());
    }

    @Test
    public void testIsDirty() {
        // Test that isDirty() always returns false
        assertFalse(NoMessageHistory.INSTANCE.isDirty());
    }
}
