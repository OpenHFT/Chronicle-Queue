/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IndexingMoveToCycleTest extends IndexingTestCommon {

    /**
     * The behaviour of moveToCycle is undefined for invalid cycles. Moving to a non-existent cycle puts the tailer into
     * an inconsistent internal state.
     */
    @Test
    @DisplayName("moveToCycle rejects negative cycle when no data exists")
    void noDataMoveToNegativeCycle() {
        assertFalse(tailer.moveToCycle(-1), "tailer should reject moveToCycle(-1) with no data");
        assertEquals(-2147483648, tailer.cycle(), "tailer cycle should remain unset after invalid cycle");
    }

    @Test
    @DisplayName("moveToCycle rejects non-existent cycle when no data exists")
    void noDataMoveToNonExistentCycle() {
        assertFalse(tailer.moveToCycle(1), "tailer should reject moveToCycle(1) with no data");
        assertEquals(-2147483648, tailer.cycle(), "tailer cycle should remain unset after missing cycle");
    }

    @Test
    @DisplayName("moveToCycle rejects missing cycle when data exists")
    void someDataMoveToNonExistentCycle() {
        appender.writeText("test");
        assertFalse(tailer.moveToCycle(1), "tailer should reject moveToCycle(1) when cycle file is missing");
        assertEquals(-2147483648, tailer.cycle(), "tailer cycle should remain unset after failed move");
        assertEquals("test", tailer.readText(), "tailer should still read existing entry after failed move");
    }
}
