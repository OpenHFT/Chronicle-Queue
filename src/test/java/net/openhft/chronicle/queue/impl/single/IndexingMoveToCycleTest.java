package net.openhft.chronicle.queue.impl.single;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IndexingMoveToCycleTest extends IndexingTestCommon {

    /**
     * The behaviour of moveToCycle is undefined for invalid cycles. Moving to a non-existent cycle puts the tailer into
     * an inconsistent internal state.
     */
    @Test
    void noDataMoveToNegativeCycle() {
        assertFalse(tailer.moveToCycle(-1));
        assertEquals(-2147483648, tailer.cycle());
    }

    @Test
    void noDataMoveToNonExistentCycle() {
        assertFalse(tailer.moveToCycle(1));
        assertEquals(-2147483648, tailer.cycle());
    }

    @Test
    void someDataMoveToNonExistentCycle() {
        appender.writeText("test");
        assertFalse(tailer.moveToCycle(1));
        assertEquals(-2147483648, tailer.cycle());
        assertEquals("test", tailer.readText());
    }
}
