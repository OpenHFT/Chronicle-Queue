/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class NoOpConditionTest {

    @Test
    public void noOpMethodsReturnImmediately() throws Exception {
        NoOpCondition c = NoOpCondition.INSTANCE;
        c.await();
        c.awaitUninterruptibly();
        assertEquals(123L, c.awaitNanos(123L));
        assertTrue(c.await(1, TimeUnit.MILLISECONDS));
        assertTrue(c.awaitUntil(new Date(System.currentTimeMillis())));
        c.signal();
        c.signalAll();
    }
}

