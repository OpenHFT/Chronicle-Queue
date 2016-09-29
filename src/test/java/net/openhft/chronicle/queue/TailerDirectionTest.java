/*
 * Copyright 2016 higherfrequencytrading.com
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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * Created by Marcus Spiegel on 29/09/16.
 */
public class TailerDirectionTest extends ChronicleQueueTestBase {

    private static final String TEST_MESSAGE_PREFIX = "Test entry: ";

    /**
     * Return a test message string for a specific ID.
     */
    private String testMessage(int id) {
        return TEST_MESSAGE_PREFIX + id;
    }

    /**
     * Add a test message with the given ExcerptAppender and return the index position of the entry
     * @param appender ExceptAppender
     * @param msg test message
     * @return index position of the entry
     */
    private long appendEntry(final ExcerptAppender appender, String msg) {
        DocumentContext dc = appender.writingDocument();
        try {
            dc.wire().write().text(msg);
            return dc.index();
        } finally {
            dc.close();
        }
    }

    /**
     * Read next message, forward or backward, depending on the settings of the Tailer
     * @param tailer ExcerptTailer
     * @return entry or null, if no entry available
     */
    private String readNextEntry(final ExcerptTailer tailer) {
        DocumentContext dc = tailer.readingDocument();
        try {
            if (dc.isPresent()) {
                return dc.wire().read().text();
            }
            return null;
        } finally {
            dc.close();
        }
    }


    //
    // Test procedure:
    // 1) Read in FORWARD direction 2 of the 4 entries
    // 2) Toggle Tailer direction and read in BACKWARD direction 2 entries
    // 3) Redo step 1)
    //
    @Test
    public void testTailerForwardBackwardRead() throws Exception {
        String basePath = OS.TARGET + "/tailerForwardBackward-" + System.nanoTime();

        ChronicleQueue queue = new SingleChronicleQueueBuilder(basePath)
                .rollCycle(RollCycles.HOURLY)
                .build();
        ExcerptAppender appender = queue.acquireAppender();
        ExcerptTailer tailer = queue.createTailer();

        //
        // Prepare test messages in queue
        //
        // Map of test messages with their queue index position
        HashMap<String,Long> msgIndexes =  new HashMap<>();

        for (int i = 0; i < 4; i++) {
            String msg = testMessage(i);
            long idx = appendEntry(appender, msg);
            msgIndexes.put(msg, idx);
        }

        Assert.assertEquals("[Forward 1] Wrong message 0", testMessage(0), readNextEntry(tailer));
        Assert.assertEquals("[Forward 1] Wrong Tailer index after reading msg 0", msgIndexes.get(testMessage(1)).longValue(), tailer.index());
        Assert.assertEquals("[Forward 1] Wrong message 1", testMessage(1), readNextEntry(tailer));
        Assert.assertEquals("[Forward 1] Wrong Tailer index after reading msg 1", msgIndexes.get(testMessage(2)).longValue(), tailer.index());

        tailer.direction(TailerDirection.BACKWARD);

        Assert.assertEquals("[Backward] Wrong message 2", testMessage(2), readNextEntry(tailer));
        Assert.assertEquals("[Backward] Wrong Tailer index after reading msg 2", msgIndexes.get(testMessage(1)).longValue(), tailer.index());
        Assert.assertEquals("[Backward] Wrong message 1", testMessage(1), readNextEntry(tailer));
        Assert.assertEquals("[Backward] Wrong Tailer index after reading msg 1", msgIndexes.get(testMessage(0)).longValue(), tailer.index());

        tailer.direction(TailerDirection.FORWARD);

        Assert.assertEquals("[Forward 2] Wrong message 0", testMessage(0), readNextEntry(tailer));
        Assert.assertEquals("[Forward 2] Wrong Tailer index after reading msg 0", msgIndexes.get(testMessage(1)).longValue(), tailer.index());
        Assert.assertEquals("[Forward 2] Wrong message 1", testMessage(1), readNextEntry(tailer));
        Assert.assertEquals("[Forward 2] Wrong Tailer index after reading msg 1", msgIndexes.get(testMessage(2)).longValue(), tailer.index());

        queue.close();
    }
}
