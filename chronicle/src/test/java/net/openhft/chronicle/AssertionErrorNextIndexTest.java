/*
 * Copyright 2013 Peter Lawrey
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

import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class AssertionErrorNextIndexTest {
    private static final String CHRONICLE
            = System.getProperty("java.io.tmpdir")
            + System.getProperty("file.separator")
            + "AssertionErrorNextIndexTest";
    private static final Random R = new Random(1);

    private static void writeToChronicle(ExcerptAppender a) {
        a.startExcerpt(1024);
        a.position(R.nextInt((int) a.capacity()) + 1);
        a.finish();
    }

    private static void readFromChronicle(ExcerptTailer t) {
        t.finish();
    }

    @Test
    @Ignore
    public void startTest() throws IOException, InterruptedException {
        ChronicleTools.deleteOnExit(CHRONICLE);

        // shrink the chronicle chunks to trigger error earlier
        final ChronicleConfig config = ChronicleConfig.TEST;
        config.indexBlockSize(1024 * 1024);
        config.dataBlockSize(4 * 1024);

        Chronicle chronicle1 = new IndexedChronicle(CHRONICLE, config);
        ExcerptAppender appender = chronicle1.createAppender();
        for (int i = 0; i < 100; i++) {
            writeToChronicle(appender);
        }
        chronicle1.close();

        // Let the writer start writing first
        long lastIndex = 0;
        long counter = 0;

        while (counter < 100) {
            Chronicle chronicle = new IndexedChronicle(CHRONICLE, config);
            ExcerptTailer tailer = chronicle.createTailer();
            boolean ok = tailer.index(lastIndex);
            int count = 10;
            while (tailer.nextIndex() && count-- > 0) {
                System.out.println(tailer.index());
                readFromChronicle(tailer);
                counter++;
            }
            lastIndex = tailer.index();
            tailer.close();
            chronicle.close();
        }
    }
}