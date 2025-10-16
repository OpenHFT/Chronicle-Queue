/*
 * Copyright 2016-2025 chronicle.software
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

package net.openhft.chronicle.queue.issue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TailerTest extends QueueTestCommon {

    public static final Path QUEUE_PATH = Paths.get(OS.getTarget() + "/host-1/queue/broker_out");
    public static final int OFFSET = 3;

    @Before
    @After
    public void cleanupFiles() {
        IOTools.deleteDirWithFiles(QUEUE_PATH.toFile());
    }

    @Test
    public void reproduce() {
        IOTools.deleteDirWithFiles(QUEUE_PATH.toFile());

        long firstOutputIndex = Long.MAX_VALUE;
        long lastOutputIndex = Long.MIN_VALUE;

        try (ChronicleQueue q = createQueue(); ExcerptAppender appender = q.createAppender(); ExcerptTailer tailer = q.createTailer()) {
            for (int i = 0; i < 5; i++) {
                final String text = "Hello World " + i;
                try (DocumentContext dc = appender.writingDocument()) {
                    dc.wire().writeText(text);
                }
                System.out.format("Appended \"%s\" at %d%n", text, appender.lastIndexAppended());
                firstOutputIndex = Math.min(firstOutputIndex, appender.lastIndexAppended());
                lastOutputIndex = Math.max(lastOutputIndex, appender.lastIndexAppended());
            }
            System.out.format("firstOutputIndex = %d%n", firstOutputIndex);
            System.out.format("lastOutputIndex = %d%n", lastOutputIndex);

            System.out.println("Reading initial");
            drainTailer(tailer);
        }

        try (ChronicleQueue q = createQueue(); ExcerptTailer tailer = q.createTailer()) {
            initRecovery(tailer, firstOutputIndex + OFFSET);
            final List<String> messages = drainTailer(tailer);
            assertEquals("Hello World " + OFFSET, messages.get(0));
        }
    }

    private List<String> drainTailer(ExcerptTailer tailer) {
        final List<String> result = new ArrayList<>();
        for (; ; ) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                final String text = dc.wire().readText();
                System.out.format("Tailed   \"%s\" at %d%n", text, dc.index());
                result.add(text);
            }
        }
        return result;
    }

    private void initRecovery(ExcerptTailer tailer, long index) {
        tailer.toStart();
        System.out.println("Initializing recovery to " + index);
        if (0 < index) {
            tailer.moveToIndex(index); // Not index + 1
        }
    }

    private ChronicleQueue createQueue() {
        return ChronicleQueue.singleBuilder(QUEUE_PATH).build();
    }
}
