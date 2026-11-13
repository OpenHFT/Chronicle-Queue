/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;

public class PretouchUtilTest extends QueueTestCommon {

    @Test
    public void createEventHandlerAndPretoucherFallback() {
        ignoreException("Pretoucher is only supported");
        final File dir = getTmpDir();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(dir).build()) {
            final EventHandler handler = PretouchUtil.createEventHandler(q);
            assertNotNull(handler);
            // Exercise handler once. In enterprise builds this may perform work and return true;
            // in OSS fallback it returns false. Only assert that it does not throw.
            try {
                handler.action();
            } catch (net.openhft.chronicle.core.threads.InvalidEventHandlerException ignored) {
                // acceptable if handler indicates closure
            }

            // Pretoucher is enterprise-only; ensure factory is initialised and does not throw creating event handler.
        }
    }

    @Test
    public void eventHandlerActionOnClosedQueueDoesNotThrow() {
        ignoreException("Pretoucher is only supported");
        final File dir = getTmpDir();
        EventHandler handler;
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(dir).build()) {
            handler = PretouchUtil.createEventHandler(q);
        }
        try {
            handler.action();
        } catch (net.openhft.chronicle.core.threads.InvalidEventHandlerException ignored) {
            // acceptable outcome when the queue is already closed
        }
    }

    @Test
    public void createPretoucherWithoutEnterpriseFailsOnceWithClearError() {
        assumeFalse(SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable());
        ignoreException("Pretoucher is only supported");
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).build()) {
            assertEnterpriseOnly(captureFailure(q::createPretoucher));
        }
    }

    @Test
    public void appenderPretouchWithoutEnterpriseFailsOnceWithClearError() {
        assumeFalse(SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable());
        ignoreException("Pretoucher is only supported");
        expectException(key -> key.throwable() instanceof UnsupportedOperationException, "appender pretouch reports the Enterprise-only failure");
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(getTmpDir()).build();
             ExcerptAppender appender = q.createAppender()) {
            assertEnterpriseOnly(captureFailure(appender::pretouch));
        }
    }

    private static Throwable captureFailure(Runnable action) {
        try {
            action.run();
        } catch (Throwable t) {
            return t;
        }
        throw new AssertionError("Expected pretouching to fail without Chronicle Queue Enterprise");
    }

    private static void assertEnterpriseOnly(Throwable thrown) {
        assertEquals("Expected UnsupportedOperationException but was " + thrown.getClass().getName(),
                UnsupportedOperationException.class, thrown.getClass());
        assertTrue(thrown.getMessage(), thrown.getMessage().contains("requires Chronicle Queue Enterprise"));
        final long factoryFrames = Arrays.stream(thrown.getStackTrace())
                .filter(e -> e.getClassName().startsWith(PretouchUtil.class.getName()) && e.getMethodName().equals("createPretoucher"))
                .count();
        assertEquals("pretoucher creation must not recurse", 2, factoryFrames);
    }
}
