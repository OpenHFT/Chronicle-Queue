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
import org.junit.jupiter.api.DisplayName;
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
    @DisplayName("Broken chain rollback preserves reader state")
    public void brokenChainQueue() {
        String tmpName = OS.getTarget() + "/brokenChain-" + System.nanoTime();
        try (ChronicleQueue queue = ChronicleQueue.single(tmpName);
             // using createAppender() doesn't work as the chained methods uses acquireAppender()
             ExcerptAppender appender = ThreadLocalAppender.acquireThreadLocalAppender(queue);
             ExcerptTailer tailer = queue.createTailer()) {

            final First writer = appender.methodWriter(First.class);
            assertTrue(appender.writingIsComplete(), "initial appender should start in writing complete state");

            List<String> list = new ArrayList<>();
            First first = pre -> msg -> list.add("pre: " + pre + ", msg: " + msg);
            MethodReader reader = tailer.methodReader(first);

            assertFalse(reader.readOne(), "initial read should not return any messages");

            appender.rollbackIfNotComplete();

            assertFalse(reader.readOne(), "reader should remain empty after rollback of incomplete write");

            Second second = writer.pre("pre");
            assertFalse(appender.writingIsComplete(), "after pre 'pre', writing should be incomplete");
            second.msg("msg");
            assertTrue(appender.writingIsComplete(), "after msg 'msg', writing should be complete");
            appender.rollbackIfNotComplete();

            assertTrue(reader.readOne(), "reader should read first message after commit");
            assertFalse(reader.readOne(), "reader should reach end after first message");
            assertEquals("[pre: pre, msg: msg]", list.toString(), "list should contain first chain message");

            list.clear();
            Second secondB = writer.pre("bad-pre");
            assertFalse(appender.writingIsComplete(), "after pre 'bad-pre', writing should be incomplete");
            appender.rollbackIfNotComplete();
            assertTrue(appender.writingIsComplete(), "after rollback of bad-pre, writing should be complete");
            assertFalse(reader.readOne(), "reader should not see messages after bad-pre rollback");
            assertEquals("[]", list.toString(), "rollback should not add side effects to list");

            Second secondC = writer.pre("pre-C");
            assertFalse(appender.writingIsComplete(), "after pre 'pre-C', writing should be incomplete");
            secondC.msg("msg-C");
            assertTrue(appender.writingIsComplete(), "after msg 'msg-C', writing should be complete");
            appender.rollbackIfNotComplete();

            assertTrue(reader.readOne(), "reader should read message after pre-C commit");
            assertFalse(reader.readOne(), "reader should reach end after pre-C message");
            assertEquals("[pre: pre-C, msg: msg-C]", list.toString(), "list should contain pre-C chain message");
        }
        IOTools.deleteDirWithFiles(tmpName);
    }
}
