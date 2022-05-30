package net.openhft.chronicle.queue;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SurefireInterruptFlagTest {

    /**
     * Test for https://issues.apache.org/jira/browse/SUREFIRE-1863
     */
    @Test
    void testSurefireLeavesInterruptFlagIntactOnOutput() {
        assertFalse(Thread.currentThread().isInterrupted());
        Thread.currentThread().interrupt();
        assertTrue(Thread.currentThread().isInterrupted());
        System.out.println("Hello world!");
        assertTrue(Thread.currentThread().isInterrupted());
    }
}
