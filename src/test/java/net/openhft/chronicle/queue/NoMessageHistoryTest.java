/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"deprecation", "removal"})
public class NoMessageHistoryTest extends QueueTestCommon {

    @Test
    @DisplayName("Singleton instance provides non-null NoMessageHistory object")
    public void testSingletonInstance() {
        // Test that the singleton instance is available and not null
        assertNotNull(NoMessageHistory.INSTANCE, "NoMessageHistory singleton instance should be available and not null");
    }

    @Test
    @DisplayName("Timings returns zero when history is disabled")
    public void testTimings() {
        // Test that timings() always returns 0
        assertEquals(0, NoMessageHistory.INSTANCE.timings(), "NoMessageHistory should return 0 timings when message history is disabled");
    }

    @Test
    @DisplayName("Timing values return -1 for all indices")
    public void testTimingForIndex() {
        // Test that timing(int n) always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.timing(0), "NoMessageHistory should return -1 for timing at index 0 when history is disabled");
        assertEquals(-1, NoMessageHistory.INSTANCE.timing(1), "NoMessageHistory should return -1 for timing at index 1 when history is disabled");
    }

    @Test
    @DisplayName("Sources returns zero when history is disabled")
    public void testSources() {
        // Test that sources() always returns 0
        assertEquals(0, NoMessageHistory.INSTANCE.sources(), "NoMessageHistory should return 0 sources when message history is disabled");
    }

    @Test
    @DisplayName("Source ID lookup returns -1 for indices")
    public void testSourceIdForIndex() {
        // Test that sourceId(int n) always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceId(0), "NoMessageHistory should return -1 for sourceId at index 0 when history is disabled");
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceId(1), "NoMessageHistory should return -1 for sourceId at index 1 when history is disabled");
    }

    @Test
    @DisplayName("Source IDs endsWith returns false without history")
    public void testSourceIdsEndsWith() {
        // Test that sourceIdsEndsWith(int[] sourceIds) always returns false
        assertFalse(NoMessageHistory.INSTANCE.sourceIdsEndsWith(new int[]{1, 2, 3}), "NoMessageHistory should return false for sourceIdsEndsWith when history is disabled");
    }

    @Test
    @DisplayName("Source index lookup returns -1 for indices")
    public void testSourceIndexForIndex() {
        // Test that sourceIndex(int n) always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceIndex(0), "NoMessageHistory should return -1 for sourceIndex at index 0 when history is disabled");
        assertEquals(-1, NoMessageHistory.INSTANCE.sourceIndex(1), "NoMessageHistory should return -1 for sourceIndex at index 1 when history is disabled");
    }

    @Test
    @DisplayName("Reset with parameters completes without side effects")
    public void testResetWithParameters() {
        // Test that reset(int sourceId, long sourceIndex) performs no action (no exceptions thrown)
        NoMessageHistory.INSTANCE.reset(1, 100L);
        assertTrue(true, "NoMessageHistory reset with sourceId and sourceIndex should complete without throwing exception");
    }

    @Test
    @DisplayName("Reset without parameters completes without side effects")
    public void testResetWithoutParameters() {
        // Test that reset() performs no action (no exceptions thrown)
        NoMessageHistory.INSTANCE.reset();
        assertTrue(true, "NoMessageHistory reset should complete without throwing exception");
    }

    @Test
    @DisplayName("Last source ID returns -1 when disabled")
    public void testLastSourceId() {
        // Test that lastSourceId() always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.lastSourceId(), "NoMessageHistory should return -1 for lastSourceId when history is disabled");
    }

    @Test
    @DisplayName("Last source index returns -1 when disabled")
    public void testLastSourceIndex() {
        // Test that lastSourceIndex() always returns -1
        assertEquals(-1, NoMessageHistory.INSTANCE.lastSourceIndex(), "NoMessageHistory should return -1 for lastSourceIndex when history is disabled");
    }

    @Test
    @DisplayName("IsDirty reports false when message history is disabled")
    public void testIsDirty() {
        // Test that isDirty() always returns false
        assertFalse(NoMessageHistory.INSTANCE.isDirty(), "NoMessageHistory should return false for isDirty when history is disabled");
    }
}
