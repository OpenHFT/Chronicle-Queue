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

package net.openhft.chronicle.tcp;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PersistedLocalVanillaChronicleTest extends PersistedChronicleTestBase {

    @Test
    public void testPersistedLocalVanillaSink_001() throws Exception {
        final int port = BASE_PORT + 301;
        final String basePath = getVanillaTestPath();

        final Chronicle chronicle = new VanillaChronicle(basePath);
        final ChronicleSource source = new ChronicleSource(chronicle, port);
        final Chronicle sink = localChronicleSink(chronicle, "localhost", port);
        final CountDownLatch latch = new CountDownLatch(5);
        final Random random = new Random();

        final int items = 100;

        try {
            Thread appenderThread = new Thread() {
                public void run() {
                    try {
                        final ExcerptAppender appender = source.createAppender();
                        for (long i = 1; i <= items; i++) {
                            if (latch.getCount() > 0) {
                                latch.countDown();
                            }

                            appender.startExcerpt(8);
                            appender.writeLong(i);
                            appender.finish();

                            sleep(10 + random.nextInt(80));

                            appender.close();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            appenderThread.start();
            latch.await();

            final ExcerptTailer tailer1 = sink.createTailer().toStart();
            for (long i = 1; i <= items; i++) {
                assertTrue(tailer1.nextIndex());
                assertEquals(i, tailer1.readLong());
                tailer1.finish();
            }

            tailer1.close();

            appenderThread.join();

            sink.close();
            sink.clear();
        } finally {
            source.close();
            source.clear();
        }
    }
}
