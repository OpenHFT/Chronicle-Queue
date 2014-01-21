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

package net.openhft.chronicle.sandbox;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.sandbox.tcp.VanillaChronicleSink;
import net.openhft.chronicle.sandbox.tcp.VanillaChronicleSource;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VanillaChronicleSourceTest {
    @Test
    public void testReplication() throws IOException {
        int RUNS = 100;

        String basePath = System.getProperty("java.io.tmpdir") + "/testReplication";
        VanillaChronicleSource chronicle = new VanillaChronicleSource(new VanillaChronicle(basePath + "-source"), 0);
        int localPort = chronicle.getLocalPort();
        VanillaChronicleSink chronicle2 = new VanillaChronicleSink(new VanillaChronicle(basePath + "-sink"), "localhost", localPort);

        try {
            ExcerptAppender appender = chronicle.createAppender();
            ExcerptTailer tailer = chronicle2.createTailer();

            assertEquals(-1L, tailer.index());
            for (int i = 0; i < RUNS; i++) {
//                if ((i & 65535) == 0)
//                    System.err.println("i: " + i);
//                if (i == 88000)
//                    Thread.yield();
                assertFalse(tailer.nextIndex());
                appender.startExcerpt();
                int value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
//                chronicle.checkCounts(1, 2);
                assertTrue("i: " + i, tailer.nextIndex());
//                chronicle2.checkCounts(1, 2);
                assertTrue("i: " + i + " remaining: " + tailer.remaining(), tailer.remaining() > 0);
                assertEquals("i: " + i, value, tailer.parseLong());
                assertEquals("i: " + i, 0, tailer.remaining());
                tailer.finish();
//                chronicle2.checkCounts(1, 2);
            }
        } finally {
            chronicle2.close();
            chronicle.clear();
        }
    }
}
