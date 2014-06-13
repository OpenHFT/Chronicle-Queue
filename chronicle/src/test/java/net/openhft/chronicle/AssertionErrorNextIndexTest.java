/*
 * Copyright 2014 Higher Frequency Trading
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle;

import net.openhft.chronicle.tools.ChronicleIndexReader;
import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static net.openhft.chronicle.IndexedChronicle1Test.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Alex Koon
 */
public class AssertionErrorNextIndexTest extends IndexedChronicleTestBase {
    private static final Random R = new Random(1);

    private static void writeToChronicle(ExcerptAppender a, int i) {
        a.startExcerpt(1024);
        final int size = R.nextInt((int) a.remaining() - 8) + 8;
        a.writeInt(i);
        a.writeInt(size);
        a.position(size);
        a.finish();
    }

    private static int readFromChronicle(ExcerptTailer t) {
        int n = t.readInt();
        int size = t.readInt();
        assertEquals(size, t.capacity());
        t.finish();
        return n;
    }

    @Test
    @Ignore
    public void assertionErrorNextIndexTest() throws IOException, InterruptedException {
        final String basePath = getTestPath();

        // shrink the chronicle chunks to trigger error earlier
        final ChronicleConfig config = ChronicleConfig.TEST;
        config.indexBlockSize(1024 * 1024);
        config.dataBlockSize(4 * 1024);

        Chronicle chronicle1 = new IndexedChronicle(basePath, config);
        ExcerptAppender appender = chronicle1.createAppender();
        for (int i = 0; i < 100; i++) {
            writeToChronicle(appender, i);
        }
        chronicle1.close();
        {
            Chronicle chronicle = new IndexedChronicle(basePath, config);
            ExcerptTailer tailer = chronicle.createTailer();
            int counter = 0;
            while (tailer.nextIndex()) {
//                System.out.println(counter+": " +tailer.index());
                assertTrue("Capacity: " + tailer.capacity(), tailer.capacity() <= 1024);
                int i = readFromChronicle(tailer);
                assertEquals(counter, i);
                counter++;
            }
            chronicle.close();
        }

        ChronicleIndexReader.main(basePath + ".index");
        // Let the writer start writing first
        long lastIndex = 0;
        long counter = 0;

        while (counter < 100) {
            Chronicle chronicle = new IndexedChronicle(basePath, config);
            ExcerptTailer tailer = chronicle.createTailer();
            System.out.println("index(" + (lastIndex - 1) + ")");
            boolean ok = tailer.index(lastIndex - 1);
            if (ok) {
                assertTrue("Capacity: " + tailer.capacity(), tailer.capacity() <= 1024);
                int i = readFromChronicle(tailer);
                assertEquals(counter - 1, i);
            }
            int count = 10;
            while (tailer.nextIndex() && count-- > 0 && counter < 100) {
                System.out.println(counter + ": " + tailer.index());
                assertTrue("counter: " + counter + ", Capacity: " + tailer.capacity(), tailer.capacity() <= 1024);
                int i = readFromChronicle(tailer);
                assertEquals(counter, i);
                counter++;
            }
            lastIndex = tailer.index();
            chronicle.close();
        }

        assertClean(basePath);
    }
}