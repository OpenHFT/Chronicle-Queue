/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.rollcycles.TestRollCycles;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.domestic.InternalWire;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import static org.junit.Assert.*;

public class DocumentAcquisitionFailureTest extends QueueTestCommon {
    @Test
    public void acquisitionErrorReleasesLockAndRestoresCount() throws Exception {
        assertAcquisitionCleanup(new AssertionError("before entering the application header"));
    }

    @Test
    public void acquisitionRuntimeExceptionReleasesLockAndRestoresCount() throws Exception {
        assertAcquisitionCleanup(new IllegalStateException("before entering the application header"));
    }

    private void assertAcquisitionCleanup(Throwable injected) throws Exception {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(getTmpDir()).testBlockSize()
                .rollCycle(TestRollCycles.TEST_DAILY).timeProvider(() -> 0).build();
             StoreAppender appender = (StoreAppender) queue.createAppender()) {
            appender.writeText("before");
            Field wireField = StoreAppender.class.getDeclaredField("wire");
            Field countField = StoreAppender.class.getDeclaredField("count");
            wireField.setAccessible(true);
            countField.setAccessible(true);
            Wire original = (Wire) wireField.get(appender);
            Wire intercepted = (Wire) Proxy.newProxyInstance(Wire.class.getClassLoader(), new Class<?>[]{Wire.class, InternalWire.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("enterHeader"))
                            throw injected;
                        try {
                            return method.invoke(original, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
            wireField.set(appender, intercepted);
            try {
                assertSame(injected, assertThrows(injected.getClass(), appender::writingDocument));
                assertFalse(queue.writeLock().locked());
                assertEquals(0, countField.getInt(appender));
            } finally {
                wireField.set(appender, original);
                if (queue.writeLock().locked())
                    queue.writeLock().unlock();
            }
            appender.writeText("after");
            try (StoreAppender another = (StoreAppender) queue.createAppender()) {
                another.writeText("another appender");
            }
            try (ExcerptTailer tailer = queue.createTailer()) {
                assertEquals("before", tailer.readText());
                assertEquals("after", tailer.readText());
                assertEquals("another appender", tailer.readText());
                assertNull(tailer.readText());
            }
        }
    }
}
