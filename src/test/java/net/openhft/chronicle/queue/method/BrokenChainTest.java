/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
