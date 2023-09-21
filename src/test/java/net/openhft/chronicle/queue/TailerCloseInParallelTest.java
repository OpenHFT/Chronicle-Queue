/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.UncheckedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.*;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import javax.print.Doc;
import java.nio.file.Paths;
import java.util.Random;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

// Run until failure (several thousand times) to detect tailer parallel closing issues
public class TailerCloseInParallelTest extends QueueTestCommon {
    static String file = OS.getTarget() + "/deleteme-" + Time.uniqueId();

    static final int size = 1 << 10;
    // blackholes to avoid code elimination.
    static int s32;
    static long s64;
    static float f32;
    static double f64;
    static String s;
    static Random random = new Random();

    @Override
    @Before
    public void threadDump() {
        super.threadDump();
    }

    @AfterClass
    public static void cleanup() {
        IOTools.deleteDirWithFiles(file, 3);
    }

    @Test
    public void runTenTimes() throws InterruptedException {
        finishedNormally = false;
        assumeTrue(OS.is64Bit());

        for (int t = 10; t >= 1; t--) {
            try {
                doPerfTest(file,
                        bytes -> writeMany(bytes, size),
                        bytes -> readMany(bytes, size),
                        2000, t == 1);
            } catch (ClosedIllegalStateException ex) {
                System.err.println("Caught expected: " + ex);
                break;
            }
        }

        Paths.get(file).toFile().delete();
        finishedNormally = true;
    }

    static void doPerfTest(String file, TestWriter<Bytes<?>> writer, TestReader<Bytes<?>> reader, int count, boolean print) throws InterruptedException {
        Histogram writeHdr = new Histogram(30, 7);
        Histogram readHdr = new Histogram(30, 7);
        try (ChronicleQueue chronicle = single(file).testBlockSize().rollCycle(RollCycles.FIVE_MINUTELY).build();
             ExcerptTailer tailer0 = chronicle.createTailer()) {

            System.err.println("End is " + Long.toHexString(tailer0.toEnd().index()));

            Thread thread = new Thread(() -> {
                tailer0.singleThreadedCheckReset();
                for (int i=0; i < random.nextInt(10); i++) {
                    Jvm.pause(1);
                    try(DocumentContext dc = tailer0.readingDocument()) {

                    }
                }
                Closeable.closeQuietly(tailer0);
            });
            thread.start();

            UncheckedBytes<BytesStore> bytes = new UncheckedBytes<>(BytesStore.empty().bytesForRead());
            try (ExcerptAppender appender = chronicle.acquireAppender()) {
                for (int i = 0; i < count; i++) {
                    long start = System.nanoTime();
                    try (DocumentContext dc = appender.writingDocument()) {
                        Bytes<?> bytes0 = dc.wire().bytes();
                        bytes0.ensureCapacity(size);
                        bytes.setBytes(bytes0);
                        bytes.readPosition(bytes.writePosition());
                        writer.writeTo(bytes);
                        bytes0.writePosition(bytes.writePosition());
                    }
                    long time = System.nanoTime() - start;
                    writeHdr.sample(time);
                }
                System.err.println("... Wrote " + Long.toHexString(appender.lastIndexAppended()));
            }

            try (ExcerptTailer tailer = chronicle.createTailer()) {
                for (int i = 0; i < count; i++) {
                    long start2 = System.nanoTime();
                    try (DocumentContext dc = tailer.readingDocument()) {
                        assertTrue(dc.isPresent());
                        Bytes<?> bytes0 = dc.wire().bytes();
                        bytes.setBytes(bytes0);
                        reader.readFrom(bytes);
                    }
                    long time2 = System.nanoTime() - start2;
                    readHdr.sample(time2);
                }
                System.err.println("... End is now " + Long.toHexString(tailer.toEnd().index()));
                if (print) {
                    System.out.println("Write latencies " + writeHdr.toMicrosFormat());
                    System.out.println("Read latencies " + readHdr.toMicrosFormat());
                }
            }
            bytes.releaseLast();
            thread.join();
        }
    }

    static void writeMany(Bytes<?> bytes, int size) {
        for (int i = 0; i < size; i += 32) {
            bytes.writeInt(i);// 4 bytes
            bytes.writeFloat(i);// 4 bytes
            bytes.writeLong(i);// 8 bytes
            bytes.writeDouble(i);// 8 bytes
            bytes.writeUtf8("Hello!!"); // 8 bytes
        }
    }

    static void readMany(Bytes<?> bytes, int size) {
        for (int i = 0; i < size; i += 32) {
            s32 = bytes.readInt();// 4 bytes
            f32 = bytes.readFloat();// 4 bytes
            s64 = bytes.readLong();// 8 bytes
            f64 = bytes.readDouble();// 8 bytes
            s = bytes.readUtf8(); // 8 bytes
            assertEquals("Hello!!", s);
        }
    }

    interface TestWriter<T> {
        void writeTo(T t);
    }

    interface TestReader<T> {
        void readFrom(T t);
    }
}

