/*
 * Copyright 2014 Higher Frequency Trading
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
 * limitations under the License.
 */

package net.openhft.chronicle;

import net.openhft.affinity.AffinitySupport;
import net.openhft.lang.io.IOTools;
import net.openhft.lang.io.VanillaMappedBytes;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VanillaChronicleTimeoutTest extends VanillaChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    @Test(timeout = 500)
    public void testAppendTimeout() throws IOException {
        final String baseDir = getTestPath();
        final VanillaChronicle chronicle = new VanillaChronicle(baseDir);

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
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test(timeout = 500)
    public void testWriteTimeout() throws IOException {
        final String baseDir = getTestPath();
        final VanillaChronicle chronicle = new VanillaChronicle(baseDir);

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
            chronicle.clear();

            assertFalse(new File(baseDir).exists());
        }
    }

    @Test(timeout = 31000)
    public void testDataCacheTimeout() throws Exception {
        final String baseDir = getTestPath();
        final DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);
        final VanillaDataCache cache = new VanillaDataCache(baseDir, 10 + 7, dateCache, 10000);

        try {
            int cycle = (int) (System.currentTimeMillis() / 1000);
            for (int j = 0; j < 5; j++) {
                int runs = 2000;
                for (int i = 0; i < runs; i++) {
                    VanillaMappedBytes buffer = cache.dataFor(cycle, AffinitySupport.getThreadId(), i, true);
                    File file = cache.fileFor(cycle, AffinitySupport.getThreadId(), i, true);

                    buffer.release(); // held by VanillaMappedCache
                    buffer.release(); // VanillaDataCache always call acquire()
                    buffer.close();

                    assertTrue(file.delete());
                }
            }
        } finally {
            cache.close();
            IOTools.deleteDir(baseDir);

            assertFalse(new File(baseDir).exists());
        }
    }
}
