/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

import static net.openhft.chronicle.queue.impl.single.ThreadLocalAppender.acquireThreadLocalAppender;

public class SingleChronicleQueueCloseTest extends QueueTestCommon {

    @Test
    public void testTailAfterClose() {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.builder(getTmpDir(), WireType.BINARY).build()) {
            final ExcerptAppender appender = queue.createAppender();
            appender.writeDocument(w -> w.write(TestKey.test).int32(1));
            Closeable.closeQuietly(queue);
            try {
                appender.writeDocument(w -> w.write(TestKey.test).int32(2));
                Assert.fail();
            } catch (IllegalStateException e) {
                // ok
            }
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

            Assert.assertSame(appender3, appender4);

            final ExcerptTailer tailer = queue.createTailer();

            Assert.assertEquals("hello1", tailer.readText());
            Assert.assertEquals("hello2", tailer.readText());
            Assert.assertEquals("hello3", tailer.readText());
            Assert.assertEquals("hello4", tailer.readText());
        }
    }
}
