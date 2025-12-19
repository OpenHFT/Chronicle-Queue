/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.method;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.ThreadLocalAppender;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BrokenChainTest extends QueueTestCommon {
    interface First {
        Second pre(String pre);
    }

    interface Second {
        void msg(String msg);
    }

    @Test
    public void brokenChainQueue() {
        String tmpName = OS.getTarget() + "/brokenChain-" + System.nanoTime();
        try (ChronicleQueue queue = ChronicleQueue.single(tmpName);
             // using createAppender() doesn't work as the chained methods uses acquireAppender()
             ExcerptAppender appender = ThreadLocalAppender.acquireThreadLocalAppender(queue);
             ExcerptTailer tailer = queue.createTailer()) {

            final First writer = appender.methodWriter(First.class);
            assertTrue(appender.writingIsComplete(), "initial: writing complete");

            List<String> list = new ArrayList<>();
            First first = pre -> msg -> list.add("pre: " + pre + ", msg: " + msg);
            MethodReader reader = tailer.methodReader(first);

            assertFalse(reader.readOne(), "initial: no messages");

            appender.rollbackIfNotComplete();

            assertFalse(reader.readOne(), "after rollback: no messages");

            Second second = writer.pre("pre");
            assertFalse(appender.writingIsComplete(), "after pre: writing not complete");
            second.msg("msg");
            assertTrue(appender.writingIsComplete(), "after msg: writing complete");
            appender.rollbackIfNotComplete();

            assertTrue(reader.readOne(), "read: first message present");
            assertFalse(reader.readOne(), "read: end of queue");
            assertEquals("[pre: pre, msg: msg]", list.toString(), "read: message list");

            list.clear();
            Second secondB = writer.pre("bad-pre");
            assertFalse(appender.writingIsComplete(), "after pre: writing not complete");
            appender.rollbackIfNotComplete();
            assertTrue(appender.writingIsComplete(), "after rollback: writing complete");
            assertFalse(reader.readOne(), "after rollback: no messages");
            assertEquals("[]", list.toString(), "after rollback: no side effects");

            Second secondC = writer.pre("pre-C");
            assertFalse(appender.writingIsComplete(), "after pre: writing not complete");
            secondC.msg("msg-C");
            assertTrue(appender.writingIsComplete(), "after msg: writing complete");
            appender.rollbackIfNotComplete();

            assertTrue(reader.readOne(), "read: message present");
            assertFalse(reader.readOne(), "read: end of queue");
            assertEquals("[pre: pre-C, msg: msg-C]", list.toString(), "read: message list");
        }
        IOTools.deleteDirWithFiles(tmpName);
    }
}
