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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.impl.DirectChronicleQueue;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * using direct chronicle to send a string
 */
/*
Threads: 1 - Write rate 24.4 M/s - Read rate 34.1 M/s
Threads: 2 - Write rate 31.8 M/s - Read rate 59.3 M/s
Threads: 3 - Write rate 47.3 M/s - Read rate 90.1 M/s
Threads: 4 - Write rate 62.8 M/s - Read rate 117.9 M/s
Threads: 5 - Write rate 77.5 M/s - Read rate 145.6 M/s
Threads: 6 - Write rate 92.0 M/s - Read rate 161.0 M/s
Threads: 7 - Write rate 107.2 M/s - Read rate 196.5 M/s
Threads: 8 - Write rate 120.2 M/s - Read rate 221.3 M/s
Threads: 9 - Write rate 136.8 M/s - Read rate 244.3 M/s
Threads: 10 - Write rate 143.6 M/s - Read rate 268.7 M/s
Threads: 11 - Write rate 161.7 M/s - Read rate 260.8 M/s
 */
public class DirectChronicleQueueStringTest {

    public static final int RUNS = 1000000;
    public static final String EXPECTED_STRING = "Hello World23456789012345678901234567890";
    public static final byte[] EXPECTED_BYTES = EXPECTED_STRING.getBytes(ISO_8859_1);
    public static final String TMP = new File("/tmp").isDirectory() ? "/tmp" : OS.getTarget();

    @Test
    public void testCreateAppender() {
        for (int r = 0; r < 2; r++) {
            long start = System.nanoTime();

            String name = TMP + "/single" + start + ".q";
            File file = new File(name);
            file.deleteOnExit();

            DirectChronicleQueue chronicle = new ChronicleQueueBuilder(name)
                    .build();

            writeSome(chronicle);

            long mid = System.nanoTime();

            DirectChronicleQueue chronicle2 = new ChronicleQueueBuilder(name)
                    .build();

            readSome(chronicle2);

            long end = System.nanoTime();
           // System.out.printf("Write rate %.1f M/s - Read rate %.1f M/s%n",
                    RUNS * 1e3 / (mid - start), RUNS * 1e3 / (end - mid));
        }
    }

    private void writeSome(DirectChronicleQueue chronicle) {
        NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(EXPECTED_BYTES.length);
        final Bytes<?> toWrite = allocate.bytes();
        for (int i = 0; i < RUNS; i++) {
            toWrite.clear();
            toWrite.write(EXPECTED_BYTES);
            toWrite.flip();
            chronicle.appendDocument(toWrite);
        }
    }

    private void readSome(DirectChronicleQueue chronicle) {
        NativeBytesStore allocate = NativeBytesStore.nativeStoreWithFixedCapacity(EXPECTED_BYTES.length);
        final Bytes<?> toRead = allocate.bytes();
        AtomicLong offset = new AtomicLong(chronicle.firstBytes());
        for (int i = 0; i < RUNS; i++) {
            toRead.clear();
            chronicle.readDocument(offset, toRead);
        }
    }

    @Test
    public void testCreateAppenderMT() {
        for (int r = 0; r < 2; r++) {
            for (int t = 1; t < Runtime.getRuntime().availableProcessors(); t++) {
                List<Future<?>> futureList = new ArrayList<>();

                List<File> files = new ArrayList<>();
                long start = System.nanoTime();
                for (int j = 0; j < t; j++) {
                    String name = TMP + "/single" + start + "-" + j + ".q";
                    File file = new File(name);
                    file.deleteOnExit();
                    files.add(file);
                    DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(name)
                            .build();

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
                for (int j = 0; j < t; j++) {
                    String name = TMP + "/single" + start + "-" + j + ".q";
                    new File(name).deleteOnExit();
                    DirectChronicleQueue chronicle = (DirectChronicleQueue) new ChronicleQueueBuilder(name)
                            .build();

                    futureList.add(ForkJoinPool.commonPool().submit(() -> {
                        readSome(chronicle);
                        return null;
                    }));
                }
                for (Future<?> future : futureList) {
                    future.get();
                }
                long end = System.nanoTime();
               // System.out.printf("Threads: %,d - Write rate %.1f M/s - Read rate %.1f M/s%n", t, t * RUNS * 1e3 / (mid - start), t * RUNS * 1e3 / (end - mid));
                for (File f : files) {
                    f.delete();
                }
            }
        }
    }
}