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

package net.openhft.chronicle.sandbox.attic;

import org.junit.Test;

import java.io.IOException;

/**
 * User: peter.lawrey
 * Date: 26/09/13
 * Time: 17:48
 */
public class RollingChronicleTest {
    @Test
    public void testAppending() throws IOException {
        /*
        int counter = 0;
        String basePath = System.getProperty("java.io.tmpdir") + "/testAppending";
        ChronicleTools.deleteDirOnExit(basePath);
        ChronicleConfig test = ChronicleConfig.TEST.clone();
        test.indexFileExcerpts(256);
        for (int k = 0; k < 500; k++) {
            RollingChronicle rc = new RollingChronicle(basePath, test);
            ExcerptAppender appender = rc.createAppender();
            Assert.assertEquals("k: " + k, (long) counter, appender.size());
            // ChronicleConfig.TEST.indexFileExcerpts() * 2 / 7
            for (int i = 0; i < 255 ; i++) {
                appender.startExcerpt();
                appender.writeInt(counter++);
                appender.finish();
                Assert.assertEquals("k: " + k + ", i: " + i, (long) counter, appender.size());
            }
            appender.close();
            rc.close();
//            ChronicleMasterReader.main(basePath + "/master");
        }
        // counter = 8192*2;

        RollingChronicle rc = new RollingChronicle(basePath, test);
        ExcerptTailer tailer = rc.createTailer();
        for (int i = 0; i < counter; i++) {
            Assert.assertTrue("i: " + i, tailer.nextIndex());
            Assert.assertEquals(i, tailer.readInt());
            tailer.finish();
        }
        rc.close();
        */
    }
}
