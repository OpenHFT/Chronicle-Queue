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
import java.util.TimeZone;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VanillaChronicleTimeoutTest extends VanillaChronicleTestBase {

    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    // *************************************************************************
    //
    // *************************************************************************

    @Test(timeout = 500)
    public void testAppendTimeout() throws IOException {
        final String baseDir = getTestPath();
        final Chronicle chronicle = ChronicleQueueBuilder.vanilla(baseDir).build();

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
        final Chronicle chronicle = ChronicleQueueBuilder.vanilla(baseDir).build();

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
    public void testDataCacheTimeout() throws IOException {
        final String baseDir = getTestPath();

        final ChronicleQueueBuilder.VanillaChronicleQueueBuilder builder =
                ChronicleQueueBuilder.vanilla(baseDir)
                        .dataCacheCapacity(10000)
                        .cleanupOnClose(false);

        final VanillaDateCache dateCache = new VanillaDateCache("yyyyMMddHHmmss", 1000, GMT);
        final VanillaDataCache dataCache = new VanillaDataCache(builder, dateCache, 10 + 7);

        try {
            int cycle = (int) (System.currentTimeMillis() / 1000);
            for (int j = 0; j < 5; j++) {
                int runs = 2000;
                for (int i = 0; i < runs; i++) {
                    VanillaMappedBytes buffer = dataCache.dataFor(cycle, AffinitySupport.getThreadId(), i, true);
                    File file = dataCache.fileFor(cycle, AffinitySupport.getThreadId(), i, true);

                    buffer.release(); // held by VanillaMappedCache
                    buffer.release(); // VanillaDataCache always call acquire()
                    buffer.close();

                    assertTrue(file.delete());
                }
            }
        } finally {
            dataCache.close();
            IOTools.deleteDir(baseDir);

            assertFalse(new File(baseDir).exists());
        }
    }
}
