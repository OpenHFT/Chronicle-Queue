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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

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

            First writer = appender.methodWriter(First.class);
            assertTrue(appender.writingIsComplete());

            List<String> list = new ArrayList<>();
            First first = pre -> msg -> list.add("pre: " + pre + ", msg: " + msg);
            MethodReader reader = tailer.methodReader(first);

            assertFalse(reader.readOne());

            appender.rollbackIfNotComplete();

            assertFalse(reader.readOne());

            Second second = writer.pre("pre");
            assertFalse(appender.writingIsComplete());
            second.msg("msg");
            assertTrue(appender.writingIsComplete());
            appender.rollbackIfNotComplete();

            assertTrue(reader.readOne());
            assertFalse(reader.readOne());
            assertEquals("[pre: pre, msg: msg]", list.toString());

            list.clear();
            Second secondB = writer.pre("bad-pre");
            assertFalse(appender.writingIsComplete());
            appender.rollbackIfNotComplete();
            assertTrue(appender.writingIsComplete());
            assertFalse(reader.readOne());
            assertEquals("[]", list.toString());

            Second secondC = writer.pre("pre-C");
            assertFalse(appender.writingIsComplete());
            secondC.msg("msg-C");
            assertTrue(appender.writingIsComplete());
            appender.rollbackIfNotComplete();

            assertTrue(reader.readOne());
            assertFalse(reader.readOne());
            assertEquals("[pre: pre-C, msg: msg-C]", list.toString());
        }
        IOTools.deleteDirWithFiles(tmpName);
    }
}
