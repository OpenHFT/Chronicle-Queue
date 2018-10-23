package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NoBytesStore;
import net.openhft.chronicle.bytes.UncheckedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.wire.DocumentContext;

import java.io.IOException;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SingleChroniclePerfMain {
    static final int count = 1_000_000;
    static final int size = 4 << 10;
    // blackholes to avoid code elimination.
    static int s32;
    static long s64;
    static float f32;
    static double f64;
    static String s;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
    }

    public static void main(String[] args) throws IOException {
        for (int t = 0; t < 2; t++) {
            doPerfTest(new TestWriter<Bytes>() {
                @Override
                public void writeTo(Bytes bytes) {
                    writeMany(bytes, size);
                }
            }, new TestReader<Bytes>() {
                @Override
                public void readFrom(Bytes bytes) {
                    readMany(bytes, size);
                }
            }, t == 0 ? 100_000 : count, t > 0);
        }
    }

    static void doPerfTest(TestWriter<Bytes> writer, TestReader<Bytes> reader, int count, boolean print) throws IOException {
        Histogram writeHdr = new Histogram(30, 7);
        Histogram readHdr = new Histogram(30, 7);
        String file = OS.TARGET + "/deleteme-" + System.nanoTime();
        try (ChronicleQueue chronicle = single(file).blockSize(64 << 20).build()) {
            ExcerptAppender appender = chronicle.createAppender();
            UncheckedBytes bytes = new UncheckedBytes(NoBytesStore.NO_BYTES);
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

            ExcerptTailer tailer = chronicle.createTailer();
            for (int i = 0; i < count; i++) {
                long start2 = System.nanoTime();
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    Bytes<?> bytes0 = dc.wire().bytes();
                    long remaining = bytes0.readRemaining();
//                    if (remaining < size)
//                        fail("remaining: " + remaining);
                    bytes.setBytes(bytes0);
                    reader.readFrom(bytes);
                }
                long time2 = System.nanoTime() - start2;
                readHdr.sample(time2);
            }
        }
        if (print) {
            System.out.println("Write latencies " + writeHdr.toMicrosFormat());
            System.out.println("Read latencies " + readHdr.toMicrosFormat());
        }
        IOTools.deleteDirWithFiles(file, 3);
    }

    static void writeMany(Bytes bytes, int size) {
        for (int i = 0; i < size; i += 32) {
            bytes.writeInt(i);// 4 bytes
            bytes.writeFloat(i);// 4 bytes
            bytes.writeLong(i);// 8 bytes
            bytes.writeDouble(i);// 8 bytes
            bytes.writeUtf8("Hello!!"); // 8 bytes
        }
    }

    static void readMany(Bytes bytes, int size) {
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
