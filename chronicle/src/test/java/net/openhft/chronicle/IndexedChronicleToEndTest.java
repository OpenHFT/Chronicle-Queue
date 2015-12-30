/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package net.openhft.chronicle;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IndexedChronicleToEndTest extends IndexedChronicleTestBase {

    @Test
    public void testToEnd() throws IOException, ClassNotFoundException, InterruptedException {
        final String basePath = getTestPath();
        final Chronicle chronicle1 = ChronicleQueueBuilder.indexed(basePath).small().build();
        final Chronicle chronicle2 = ChronicleQueueBuilder.indexed(basePath).small().build();

        try {
            appendSome(chronicle1);
            showLast(chronicle2);
        } finally {
            chronicle1.close();
            chronicle2.close();
            assertClean(basePath);
        }
    }

    private static void appendSome(Chronicle queue) throws IOException {
        ExcerptAppender appender = queue.createAppender();
        for (int i = 0; i < 10; i++) {
            appender.startExcerpt(100);
            appender.writeObject(i);
            appender.finish();
        }

        appender.close();
    }

    private static void showLast(Chronicle queue) throws IOException {
        ExcerptTailer tailer = queue.createTailer().toEnd();
        assertFalse("Should not find any more excerpts", tailer.nextIndex());
        assertEquals("Expected 9", 9, tailer.index());

        tailer.close();
    }
}
