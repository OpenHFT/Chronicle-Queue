/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://chronicle.software
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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.wire.WireKey;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

public class ChronicleQueueTest {

    public static final int RUNS = 1000000;
    public static final String TMP = OS.getTarget();

    @Ignore("ignored only because this is a performance test")
    @Test
    public void testCreateAppender() {
        for (int r = 0; r < 2; r++) {
            for (int t = 1; t <= Runtime.getRuntime().availableProcessors(); t++) {
                List<Future<?>> futureList = new ArrayList<>();
                List<File> files = new ArrayList<>();
                long start = System.nanoTime();
                for (int j = 0; j < 4; j++) {
                    String name = TMP + "/single" + start + "-" + j + ".q";
                    File file = new File(name);
                    files.add(file);
                    file.deleteOnExit();
                    ChronicleQueue chronicle = new ChronicleQueueBuilder(name).build();

                    futureList.add(ForkJoinPool.commonPool().submit(() -> {
                        writeSome(chronicle);
                        return null;
                    }));
                }
                for (Future<?> future : futureList) {
                    future.get();
                }
                futureList.clear();
                long mid = System.nanoTime();
                for (int j = 0; j < 4; j++) {
                    String name = TMP + "/single" + start + "-" + j + ".q";
                    new File(name).deleteOnExit();
                    ChronicleQueue chronicle = new ChronicleQueueBuilder(name).build();

                    futureList.add(ForkJoinPool.commonPool().submit(() -> {
                        readSome(chronicle);
                        return null;
                    }));
                }
                for (Future<?> future : futureList) {
                    future.get();
                }
                long end = System.nanoTime();
                System.out.printf("Threads: %,d - Write rate %.1f M/s - Read rate %.1f M/s%n", t, t * RUNS * 1e3 / (mid - start), t * RUNS * 1e3 / (end - mid));
                for (File f : files) {
                    f.delete();
                }
            }
        }
    }

    private void readSome(ChronicleQueue chronicle) throws IOException {
        ExcerptTailer tailer = chronicle.createTailer();
        StringBuilder sb = new StringBuilder();
        // TODO check this is still needed in future versions.

        for (int i = 0; i < RUNS; i++) {
            assertTrue(tailer.readDocument(wireIn -> wireIn.read(TestKey.test).text(sb)));
        }
    }

    private void writeSome(ChronicleQueue chronicle) throws IOException {
        ExcerptAppender appender = chronicle.acquireAppender();
        for (int i = 0; i < RUNS; i++) {
            appender.writeDocument(wire -> wire.write(TestKey.test).text("Hello World23456789012345678901234567890"));
        }
    }

    enum TestKey implements WireKey {
        test
    }
}