/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import static net.openhft.chronicle.queue.RollCycles.TEST_DAILY;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class LastIndexAppendedTest {

    @Test
    public void testLastIndexAppendedAcrossRestarts() throws Exception {
        String path = OS.TARGET + "/deleteme.q-" + System.nanoTime();

        for (int i = 0; i < 5; i++) {
            try (SingleChronicleQueue queue = ChronicleQueueBuilder.single(path)
                    .rollCycle(TEST_DAILY)
                    .build()) {
                ExcerptAppender appender = queue.acquireAppender();

                try (DocumentContext documentContext = appender.writingDocument()) {
                    int index = (int) documentContext.index();
                    assertEquals(i, index);

                    documentContext.wire().write().text("hello world");
                }

                assertEquals(i, (int) appender.lastIndexAppended());
            }
        }
        try {
            IOTools.deleteDirWithFiles(path, 2);
        } catch (Exception index) {
        }
    }
}