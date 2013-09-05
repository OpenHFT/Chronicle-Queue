/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import net.openhft.chronicle.tcp.InProcessChronicleSink;
import net.openhft.chronicle.tcp.InProcessChronicleSource;
import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Test;

import java.io.IOException;

/**
 * @author peter.lawrey
 */
public class ChainedInProcessChronicleTest {
    private static final String TMP = System.getProperty("java.io.tmpdir");

    @Test
    public void testChained() throws IOException {
        ChronicleTools.deleteOnExit(TMP + "/chronicle1");
        Chronicle chronicle1 = new IndexedChronicle(TMP + "/chronicle1");
        InProcessChronicleSource source1 = new InProcessChronicleSource(chronicle1, 61111);

        ChronicleTools.deleteOnExit(TMP + "/chronicle2");
        Chronicle chronicle2 = new IndexedChronicle(TMP + "/chronicle2");
        InProcessChronicleSource source2 = new InProcessChronicleSource(chronicle2, 62222);
        InProcessChronicleSink sink2 = new InProcessChronicleSink(source2, "localhost", 61111);

        ChronicleTools.deleteOnExit(TMP + "/chronicle3");
        Chronicle chronicle3 = new IndexedChronicle(TMP + "/chronicle3");
        InProcessChronicleSink sink3 = new InProcessChronicleSink(chronicle3, "localhost", 62222);

        ExcerptAppender excerpt1 = source1.createAppender();
        Excerpt excerpt2 = sink2.createExcerpt();
        Excerpt excerpt3 = sink3.createExcerpt();

        for (int i = 1; i < 20; i++) {
            excerpt1.startExcerpt(8);
            excerpt1.writeLong(System.nanoTime());
            excerpt1.finish();

            while (excerpt2.size() < i)
                excerpt2.nextIndex();

            while (excerpt3.size() < i)
                excerpt3.nextIndex();
        }

        sink3.close();
        sink2.close();
        source1.close();
    }
}
