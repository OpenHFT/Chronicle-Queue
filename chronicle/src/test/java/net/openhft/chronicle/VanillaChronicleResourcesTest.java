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

import static org.junit.Assert.assertTrue;

public class VanillaChronicleResourcesTest {
    public static final String TMP_PATH = System.getProperty("java.io.tmpdir");

    private String getTemporaryPath(String id) {
        final File f = new File(TMP_PATH + "/VanillaChronicleResourcesTest/" + id);
        f.delete();

        return f.getAbsolutePath();
    }

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testResourcesCleanup1() throws IOException {
        final String basedir = getTemporaryPath("testResources");
        final VanillaChronicle chronicle = new VanillaChronicle(basedir);

        ExcerptAppender appender1 = chronicle.createAppender();
        appender1.startExcerpt();
        appender1.writeInt(1);
        appender1.finish();
        chronicle.checkCounts(0, 2);

        ExcerptAppender appender2 = chronicle.createAppender();
        appender2.startExcerpt();
        appender2.writeInt(2);
        appender2.finish();
        chronicle.checkCounts(0, 2);

        assertTrue(appender1 == appender1);

        appender1.close();
        chronicle.checkCounts(0, 1);

        ExcerptTailer tailer = chronicle.createTailer();
        assertTrue(tailer.nextIndex());
        chronicle.checkCounts(0, 2);
        tailer.close();

        chronicle.checkCounts(0, 1);
        chronicle.close();

        chronicle.clear();
    }
}
