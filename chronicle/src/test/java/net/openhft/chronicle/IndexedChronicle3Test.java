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

import net.openhft.chronicle.tools.SafeExcerptAppender;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexedChronicle3Test extends IndexedChronicleTestBase {

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testSafeIndexedChronicle_001() throws IOException {
        final ChronicleConfig cfg = ChronicleConfig.DEFAULT.clone();
        cfg.useSafeBuffer(true);
        cfg.leazySafeBuffer(false);
        cfg.safeBufferSize(128 * 1024);

        final String basePath = getTestPath();
        final Chronicle chronicle = new IndexedChronicle(basePath,cfg);

        ExcerptAppender appender = chronicle.createAppender();
        assertTrue(appender instanceof SafeExcerptAppender);
        appender.startExcerpt();
        appender.writeLong(1);
        appender.finish();

        ExcerptTailer tailer = chronicle.createTailer().toStart();
        assertTrue(tailer.nextIndex());
        assertEquals(1, tailer.readLong());
        assertEquals(1, tailer.readLong(0));
        tailer.finish();

        appender.close();
        tailer.close();

        chronicle.close();

        assertClean(basePath);
    }
}
