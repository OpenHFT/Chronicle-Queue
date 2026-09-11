/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueueSystemPropertiesTest {
    @Test
    @SuppressWarnings("deprecation")
    public void legacyFieldAndAccessorsShareTheSameSetting() {
        boolean previous = QueueSystemProperties.CHECK_INDEX;
        try {
            QueueSystemProperties.CHECK_INDEX = false;
            assertFalse(QueueSystemProperties.checkIndex());
            QueueSystemProperties.CHECK_INDEX = true;
            assertTrue(QueueSystemProperties.checkIndex());
            QueueSystemProperties.setCheckIndex(false);
            assertFalse(QueueSystemProperties.CHECK_INDEX);
            QueueSystemProperties.setCheckIndex(true);
            assertTrue(QueueSystemProperties.CHECK_INDEX);
        } finally {
            QueueSystemProperties.CHECK_INDEX = previous;
        }
    }
}
