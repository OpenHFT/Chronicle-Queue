/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single.jira;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class Queue28Test extends QueueTestCommon {

    private final WireType wireType;

    public Queue28Test(WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // {WireType.TEXT},
                {WireType.BINARY}
        });
    }

    /*
     * Tailer doesn't work if created before the appender
     *
     * See https://higherfrequencytrading.atlassian.net/browse/QUEUE-28
     */

    @Test
    public void test() {
        File dir = getTmpDir();
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.builder(dir, wireType)
                .testBlockSize()
                .build();
             final ExcerptAppender appender = queue.createAppender()) {

            final ExcerptTailer tailer = queue.createTailer();
            assertFalse(tailer.readDocument(r -> r.read(TestKey.test).int32()));

            appender.writeDocument(w -> w.write(TestKey.test).int32(1));
            Jvm.pause(100);
            assertTrue(tailer.readDocument(r -> r.read(TestKey.test).int32()));
        }
    }
}
