/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class PretouchUtilTest extends QueueTestCommon {

    @Test
    @DisplayName("Pretouch handler creation falls back safely")
    public void createEventHandlerAndPretoucherFallback() {
        ignoreException("Pretoucher is only supported");
        final File dir = getTmpDir();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(dir).build()) {
            final EventHandler handler = PretouchUtil.createEventHandler(q);
            assertNotNull(handler, "Pretouch handler should be created for queue");
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
    @DisplayName("Pretouch handler on closed queue does not throw")
    public void eventHandlerActionOnClosedQueueDoesNotThrow() {
        ignoreException("Pretoucher is only supported");
        final File dir = getTmpDir();
        EventHandler handler;
        try (ChronicleQueue q = SingleChronicleQueueBuilder.binary(dir).build()) {
            handler = PretouchUtil.createEventHandler(q);
        }
        assertNotNull(handler, "Pretouch handler should be created for closed queue");
        try {
            handler.action();
        } catch (net.openhft.chronicle.core.threads.InvalidEventHandlerException ignored) {
            // acceptable outcome when the queue is already closed
        }
    }
}
