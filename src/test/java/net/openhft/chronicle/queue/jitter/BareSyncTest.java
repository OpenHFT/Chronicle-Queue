/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.jitter;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BareSyncTest extends QueueTestCommon {
    @Test
    @DisplayName("Bare sync calls succeed around writes")
    public void sync() {
        try (ChronicleQueue cq = ChronicleQueue.single(OS.getTarget() + "/bare-sync-test");
             ExcerptAppender appender = cq.createAppender()) {
            appender.sync();
            appender.writeBytes(BytesStore.wrap(new byte[1024]));
            appender.sync();
        }
        assertTrue(true, "Sync operations should complete without exception before and after write"); // if we got here without an exception, the test passes
    }
}
