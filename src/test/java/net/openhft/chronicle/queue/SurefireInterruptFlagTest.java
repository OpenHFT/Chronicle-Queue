/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SurefireInterruptFlagTest extends QueueTestCommon {

    /**
     * Test for https://issues.apache.org/jira/browse/SUREFIRE-1863
     */
    @Test
    @DisplayName("Surefire keeps interrupt flag after stdout")
    void testSurefireLeavesInterruptFlagIntactOnOutput() {
        assertFalse(Thread.currentThread().isInterrupted(), "precondition: interrupt flag clear");
        Thread.currentThread().interrupt();
        assertTrue(Thread.currentThread().isInterrupted(), "interrupt flag should be set after interrupt");
        System.out.println("Hello world!");
        assertTrue(Thread.currentThread().isInterrupted(), "interrupt flag remains set after stdout");
    }
}
