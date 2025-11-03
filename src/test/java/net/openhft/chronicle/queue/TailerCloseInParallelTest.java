/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.UncheckedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Random;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

// Run until failure (several thousand times) to detect tailer parallel closing issues
public class TailerCloseInParallelTest extends QueueTestCommon {
    private static String file = OS.getTarget() + "/deleteme-" + Time.uniqueId();

    private static final int size = 1 << 10;
    // blackholes to avoid code elimination.
    private static int s32;
    private static long s64;
    private static float f32;
    private static double f64;
    private static String s;
    private static Random random = new Random();

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

    private static void doPerfTest(String file, TestWriter<Bytes<?>> writer, TestReader<Bytes<?>> reader, int count, boolean print) throws InterruptedException {
        Histogram writeHdr = new Histogram(30, 7);
        Histogram readHdr = new Histogram(30, 7);
        try (ChronicleQueue chronicle = single(file).testBlockSize().rollCycle(RollCycles.FIVE_MINUTELY).build();
             ExcerptTailer tailer0 = chronicle.createTailer()) {

            System.err.println("End is " + Long.toHexString(tailer0.toEnd().index()));

            Thread thread = new Thread(() -> {
                tailer0.singleThreadedCheckReset();
                for (int i = 0; i < random.nextInt(10); i++) {
                    Jvm.pause(1);
                    try (DocumentContext dc = tailer0.readingDocument()) {
                        assertNotNull(dc);
                    }
                }
                Closeable.closeQuietly(tailer0);
            });
            thread.start();

            Bytes<Object> underlyingBytes = BytesStore.empty().bytesForRead();
            UncheckedBytes<BytesStore<?, ?>> bytes = new UncheckedBytes<>(underlyingBytes);
            try (ExcerptAppender appender = chronicle.createAppender()) {
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

    private static void writeMany(Bytes<?> bytes, int size) {
        for (int i = 0; i < size; i += 32) {
            bytes.writeInt(i); // 4 bytes
            bytes.writeFloat(i); // 4 bytes
            bytes.writeLong(i); // 8 bytes
            bytes.writeDouble(i); // 8 bytes
            bytes.writeUtf8("Hello!!"); // 8 bytes
        }
    }

    private static void readMany(Bytes<?> bytes, int size) {
        for (int i = 0; i < size; i += 32) {
            s32 = bytes.readInt(); // 4 bytes
            f32 = bytes.readFloat(); // 4 bytes
            s64 = bytes.readLong(); // 8 bytes
            f64 = bytes.readDouble(); // 8 bytes
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
