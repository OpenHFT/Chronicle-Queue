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

package net.openhft.chronicle.examples;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.tools.ChronicleTools;

import java.io.IOException;

/**
 * 68,000,000 inserts took 7.710 seconds on i7-4500 laptop with SSD.
 *
 * @author peter.lawrey
 */
public class TestManyUpdates2Main {
    public static void main(String... ignored) throws IOException {
        ChronicleTools.warmup();
        String basePath = System.getProperty("java.io.tmpdir") + "/updates";
        ChronicleTools.deleteOnExit(basePath);
        long start = System.nanoTime();
        IndexedChronicle chronicle = new IndexedChronicle(basePath);
        int count = 68 * 1000 * 1000;
        for (ExcerptAppender e = chronicle.createAppender(); e.index() < count; ) {
            e.startExcerpt();
            e.writeUTFΔ("Message Type.");
            e.writeLong(System.currentTimeMillis());
            for (int i = 0; i < 6; i++)
                e.writeCompactDouble(i);
            e.finish();
        }
        chronicle.close();
        long time = System.nanoTime() - start;
        System.out.printf("%,d inserts took %.3f seconds%n", count, time / 1e9);
    }
}
