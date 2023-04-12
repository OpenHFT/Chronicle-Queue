/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueOut;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IncompleteMessageTest extends QueueTestCommon {
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void incompleteMessageShouldBeSkipped() throws Exception {
        System.setProperty("queue.force.unlock.mode", "ALWAYS");
        expectException("Couldn't acquire write lock after ");
        expectException("Forced unlock for the lock ");
        ignoreException("Unable to release the lock");
        try (SingleChronicleQueue queue = createQueue()) {
            try (ExcerptAppender appender = queue.acquireAppender()) {
                appender.writeDocument("hello", ValueOut::text);

                // open a document context, but do not close
                final DocumentContext documentContext = appender.writingDocument();
                documentContext.wire().bytes().write("incomplete longer write".getBytes(StandardCharsets.UTF_8));
            }
        }

        try (SingleChronicleQueue queue = createQueue()) {
            try (ExcerptAppender appender = queue.acquireAppender()) {
                appender.writeDocument("world", ValueOut::text);
            }

            try (ExcerptTailer tailer = queue.createTailer()) {
                tailer.toStart();
                assertEquals("hello", tailer.readText());
                assertEquals("world", tailer.readText());
                assertFalse(tailer.readingDocument().isPresent());
            }
        } finally {
            System.clearProperty("queue.force.unlock.mode");
        }
    }

    private SingleChronicleQueue createQueue() {
        return SingleChronicleQueueBuilder.binary(tmpDir.getRoot()).timeoutMS(250).build();
    }
}
