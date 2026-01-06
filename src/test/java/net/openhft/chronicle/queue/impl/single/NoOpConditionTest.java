/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class NoOpConditionTest {

    @Test
    @DisplayName("No-op condition methods return without blocking")
    public void noOpMethodsReturnImmediately() throws Exception {
        NoOpCondition c = NoOpCondition.INSTANCE;
        c.await();
        c.awaitUninterruptibly();
        assertEquals(123L, c.awaitNanos(123L), "awaitNanos should return requested nanos without waiting");
        assertTrue(c.await(1, TimeUnit.MILLISECONDS), "await should return true without blocking");
        assertTrue(c.awaitUntil(new Date(System.currentTimeMillis())), "awaitUntil should return true without blocking");
        c.signal();
        c.signalAll();
    }
}
