/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class NoOpWriteLockTest extends QueueTestCommon {

    @Test
    public void lockUnlockAndCloseDoNotThrow() {
        WriteLock lock = WriteLock.NO_OP;
        lock.lock();
        lock.unlock();
        lock.close();
    }

    @Test
    public void forceUnlockIfProcessIsDeadReturnsTrue() {
        assertTrue(WriteLock.NO_OP.forceUnlockIfProcessIsDead());
    }

    @Test
    public void isLockedByCurrentProcessReturnsTrueWithoutCallingConsumer() {
        AtomicBoolean called = new AtomicBoolean();
        assertTrue(WriteLock.NO_OP.isLockedByCurrentProcess(pid -> called.set(true)));
        assertFalse(called.get());
    }
}
