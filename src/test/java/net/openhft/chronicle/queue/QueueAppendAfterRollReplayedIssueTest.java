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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.Assert.assertNotNull;

/*
    Scenario:
    Queue is reloaded with roll files present and
    writingDocument retrieved with acquireDocument
 */
public class QueueAppendAfterRollReplayedIssueTest extends QueueTestCommon {

    @Test
    public void test() {
        int messages = 10;

        String path = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();
        SetTimeProvider timeProvider = new SetTimeProvider();
        try (final ChronicleQueue writeQueue = ChronicleQueue
                .singleBuilder(path)
                .testBlockSize()
                .timeProvider(timeProvider)
                .rollCycle(TEST_SECONDLY).build();
             ExcerptAppender appender = writeQueue.createAppender()) {
            for (int i = 0; i < messages; i++) {
                timeProvider.advanceMillis(i * 100);
                Map<String, Object> map = new HashMap<>();
                map.put("key", i);
                appender.writeMap(map);
            }
        }

        timeProvider.advanceMillis(1000);

        try (final ChronicleQueue queue = ChronicleQueue
                .singleBuilder(path)
                .testBlockSize()
                .timeProvider(timeProvider)
                .rollCycle(TEST_SECONDLY).build();
             final ExcerptAppender excerptAppender = queue.createAppender()) {
            try (final DocumentContext documentContext = excerptAppender.acquireWritingDocument(false)) {
                assertNotNull(documentContext.wire());
            }

        } finally {
            try {
                IOTools.deleteDirWithFiles(path, 2);
            } catch (IORuntimeException todoFixOnWindows) {

            }
        }
    }
}
