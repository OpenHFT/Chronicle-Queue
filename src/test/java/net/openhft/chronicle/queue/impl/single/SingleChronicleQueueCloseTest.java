/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static net.openhft.chronicle.queue.impl.single.ThreadLocalAppender.acquireThreadLocalAppender;

public class SingleChronicleQueueCloseTest extends QueueTestCommon {

    @Test
    public void testTailAfterClose() {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.builder(getTmpDir(), WireType.BINARY).build()) {
            final ExcerptAppender appender = queue.createAppender();
            appender.writeDocument(w -> w.write(TestKey.test).int32(1));
            Closeable.closeQuietly(queue);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> appender.writeDocument(w -> w.write(TestKey.test).int32(2)));
        }
    }

    /**
     * NOTE: Still uses thread local appender as that is the intent of the test.
     */
    @Test
    public void reacquireAppenderAfterClose() {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.builder(getTmpDir(), WireType.BINARY).build()) {
            final ExcerptAppender appender = acquireThreadLocalAppender(queue);
            appender.writeText("hello1");
            appender.close();

            final ExcerptAppender appender2 = acquireThreadLocalAppender(queue);
            appender2.writeText("hello2");
            appender.close();

            final ExcerptAppender appender3 = acquireThreadLocalAppender(queue);
            appender2.writeText("hello3");

            final ExcerptAppender appender4 = acquireThreadLocalAppender(queue);
            appender2.writeText("hello4");

            Assertions.assertSame(appender3, appender4, "appender4");

            final ExcerptTailer tailer = queue.createTailer();

            Assertions.assertEquals("hello1", tailer.readText(), "tailer.readText()");
            Assertions.assertEquals("hello2", tailer.readText(), "tailer.readText()");
            Assertions.assertEquals("hello3", tailer.readText(), "tailer.readText()");
            Assertions.assertEquals("hello4", tailer.readText(), "tailer.readText()");
        }
    }
}
