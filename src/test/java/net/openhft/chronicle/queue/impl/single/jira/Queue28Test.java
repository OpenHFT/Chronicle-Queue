/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.jira;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Queue28Test extends QueueTestCommon {

    /*
     * Tailer doesn't work if created before the appender
     *
     * See https://higherfrequencytrading.atlassian.net/browse/QUEUE-28
     */
    @ParameterizedTest(name = "wireType={0}")
    @EnumSource(value = WireType.class, names = "BINARY")
    public void test(WireType wireType) {
        File dir = getTmpDir();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.builder(dir, wireType)
                .testBlockSize()
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            final ExcerptTailer tailer = queue.createTailer();
            assertFalse(tailer.readDocument(r -> r.read(TestKey.test).int32()), "tailer: no document before write");

            appender.writeDocument(w -> w.write(TestKey.test).int32(1));
            Jvm.pause(100);
            assertTrue(tailer.readDocument(r -> r.read(TestKey.test).int32()), "tailer: reads document after write");
        }
    }
}
