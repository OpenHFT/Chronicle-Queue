/*
 * Copyright 2014 Higher Frequency Trading
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
 * limitations under the License.
 */
package net.openhft.chronicle;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class VanillaChronicleResourcesTest extends VanillaChronicleTestBase {

    @Test
    public void testResourcesCleanup1() throws IOException {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicle chronicle = new VanillaChronicle(baseDir);
        chronicle.clear();

        try {
            final ExcerptAppender appender1 = chronicle.createAppender();
            appender1.startExcerpt();
            appender1.writeInt(1);
            appender1.finish();
            chronicle.checkCounts(1, 2);

            final ExcerptAppender appender2 = chronicle.createAppender();
            appender2.startExcerpt();
            appender2.writeInt(2);
            appender2.finish();
            chronicle.checkCounts(1, 2);

            assertTrue(appender1 == appender2);

            appender2.close();

            chronicle.checkCounts(1, 1);

            final ExcerptTailer tailer = chronicle.createTailer();
            assertTrue(tailer.nextIndex());
            chronicle.checkCounts(1, 2);

            tailer.close();

            chronicle.checkCounts(1, 1);
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test
    public void testResourcesCleanup2() throws Exception {
        final String baseDir = getTestPath();
        assertNotNull(baseDir);

        final VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.dataBlockSize(64);
        config.indexBlockSize(64);

        final VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
        chronicle.clear();

        try {

            final ExcerptAppender appender = chronicle.createAppender();
            for (int counter = 0; counter < 100; counter++) {
                appender.startExcerpt(20);
                appender.writeUTF("data-" + counter);
                appender.finish();
            }

            appender.close();

            chronicle.checkCounts(1,1);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            chronicle.close();
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }
}
