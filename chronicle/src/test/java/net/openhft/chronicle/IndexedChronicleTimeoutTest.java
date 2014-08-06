/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;

public class IndexedChronicleTimeoutTest extends IndexedChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    @Test(timeout = 1000)
    public void testAppendTimeout() throws IOException {
        final String baseDir = getTestPath();
        final Chronicle chronicle = new IndexedChronicle(baseDir);

        try {
            final ExcerptAppender appender = chronicle.createAppender();
            for (int i = 0; i < 1000; i++) {
                appender.startExcerpt();
                appender.append(1000000000 + i);
                appender.finish();
            }

            appender.close();
        } finally {
            chronicle.close();

            ChronicleTools.deleteOnExit(baseDir);
            assertFalse(new File(baseDir + ".index").exists());
            assertFalse(new File(baseDir + ".data").exists());
        }
    }

    @Test(timeout = 500)
    public void testWriteTimeout() throws IOException {
        final String baseDir = getTestPath();
        final Chronicle chronicle = new IndexedChronicle(baseDir);

        try {
            final ExcerptAppender appender = chronicle.createAppender();
            for (int i = 0; i < 1000; i++) {
                appender.startExcerpt(8);
                appender.writeLong(1000000000 + i);
                appender.finish();
            }

            appender.close();
        } finally {
            chronicle.close();

            ChronicleTools.deleteOnExit(baseDir);
            assertFalse(new File(baseDir + ".index").exists());
            assertFalse(new File(baseDir + ".data").exists());
        }
    }

    @Test(timeout = 500)
    public void testWriteReadTimeout() throws IOException {
        final String baseDir = getTestPath();
        final Chronicle chronicle = new IndexedChronicle(baseDir);

        try {
            final ExcerptAppender appender = chronicle.createAppender();
            for (int i = 0; i < 1000; i++) {
                appender.startExcerpt(8);
                appender.writeLong(1000000000 + i);
                appender.finish();
            }

            appender.close();
        } finally {
            chronicle.close();

            ChronicleTools.deleteOnExit(baseDir);
            assertFalse(new File(baseDir + ".index").exists());
            assertFalse(new File(baseDir + ".data").exists());
        }
    }
}
