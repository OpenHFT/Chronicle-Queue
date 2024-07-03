package net.openhft.chronicle.queue.impl.single;


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Exercises linear scan logic and check for regressions. The tests here are looking to check how much linear scanning is
 * going on during normal write and read use cases and to determine whether the linear scan count is as expected or excessive.
 */
class IndexingReadWritePositionTests extends IndexingTestCommon {

    @Test
    void writeAndReadUseCase() {
        appender.writeText("1");
        Indexing indexing = indexing(); // Fetch the handle to SCQIndex *after* the first write otherwise store will be null
        appender.writeText("2");
        appender.writeText("3");
        tailer.readText();
        assertZeroScans(indexing);
    }

    @Test
    void writeOnce() {
        appender.writeText("1");
        Indexing indexing = indexing(); // Fetch the handle to SCQIndex *after* the first write otherwise store will be null
        assertZeroScans(indexing);
    }

    @Test
    void writeMultipleTimes() {
        appender.writeText("1");
        Indexing indexing = indexing(); // Fetch the handle to SCQIndex *after* the first write otherwise store will be null
        appender.writeText("2");
        appender.writeText("3");
        assertZeroScans(indexing);
    }

    private void assertZeroScans(Indexing indexing) {
        assertEquals(0, indexing.linearScanCount(), "linearScanCount should be 0");
        assertEquals(0, indexing.linearScanByPositionCount(), "linearScanByPositionCount should be 0");
    }

    /**
     * Fetch the active instance of SCQIndexing from within the appender store.
     */
    private Indexing indexing() {
        if (appender.store == null) {
            fail("Appender has not been fully instantiated, please append some data first so that the store is created");
        }
        return appender.store.indexing;
    }

}
