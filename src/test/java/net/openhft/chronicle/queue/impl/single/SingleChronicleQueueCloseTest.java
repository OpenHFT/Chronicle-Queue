/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static net.openhft.chronicle.queue.impl.single.ThreadLocalAppender.acquireThreadLocalAppender;

public class SingleChronicleQueueCloseTest extends QueueTestCommon {

    @Test
    @DisplayName("Appender operations fail after queue close")
    public void testTailAfterClose() {
        try (final ChronicleQueue queue = SingleChronicleQueueBuilder.builder(getTmpDir(), WireType.BINARY).build()) {
            final ExcerptAppender appender = queue.createAppender();
            appender.writeDocument(w -> w.write(TestKey.test).int32(1));
            Closeable.closeQuietly(queue);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> appender.writeDocument(w -> w.write(TestKey.test).int32(2)),
                    "appender should reject writes after queue is closed");
        }
    }

    /**
     * NOTE: Still uses thread local appender as that is the intent of the test.
     */
    @Test
    @DisplayName("Thread-local appender can be reacquired after close")
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

            Assertions.assertSame(appender3, appender4, "thread-local appender should be reused after close");

            final ExcerptTailer tailer = queue.createTailer();

            Assertions.assertEquals("hello1", tailer.readText(), "tailer should read first message after reopen");
            Assertions.assertEquals("hello2", tailer.readText(), "tailer should read second message after reopen");
            Assertions.assertEquals("hello3", tailer.readText(), "tailer should read third message after reopen");
            Assertions.assertEquals("hello4", tailer.readText(), "tailer should read fourth message after reopen");
        }
    }
}
