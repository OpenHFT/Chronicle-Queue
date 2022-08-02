package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.UncheckedBytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import net.openhft.chronicle.core.io.ClosedIllegalStateException;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// Run until failure (several thousand times) to detect tailer parallel closing issues
public class TailerCloseInParallelTest extends QueueTestCommon {
    static String file = OS.getTarget() + "/deleteme-" + Time.uniqueId();

    static final int size = 4 << 10;
    // blackholes to avoid code elimination.
    static int s32;
    static long s64;
    static float f32;
    static double f64;
    static String s;
    static Random random = new Random();

    static volatile ChronicleQueue chronicle;
    static volatile ExcerptTailer tailer;

    @AfterClass
    public static void cleanup() {
        IOTools.deleteDirWithFiles(file, 3);
    }

    @Test
    public void runOnce() throws IOException {
        ignoreException("Reference tracing disabled");
        AbstractCloseable.disableCloseableTracing();
        AbstractReferenceCounted.disableReferenceTracing();
        Thread thread = new Thread(() -> {
            Jvm.pause(Math.min(random.nextInt(100), random.nextInt(100)));
            if (tailer != null && !tailer.isClosing()) {
                tailer.close();
            }
        });
        thread.start();
        for (int t = 0; t < random.nextInt(100); t++) {
            try {
                doPerfTest(file,
                        bytes -> writeMany(bytes, size),
                        bytes -> readMany(bytes, size),
                        1, t > 0);
            } catch (ClosedIllegalStateException ex) {
                System.err.println("Caught expected: " + ex);
                break;
            }
        }

        if (chronicle != null && !chronicle.isClosing()) {
            chronicle.close();
        }

        Paths.get(file).toFile().delete();
    }

    static void doPerfTest(String file, TestWriter<Bytes<?>> writer, TestReader<Bytes<?>> reader, int count, boolean print) throws IOException {
        Histogram writeHdr = new Histogram(30, 7);
        Histogram readHdr = new Histogram(30, 7);
        chronicle = single(file).blockSize(64 << 20).rollCycle(RollCycles.FIVE_MINUTELY).build();
        tailer = chronicle.createTailer();
        System.err.println("Length is " + tailer.toEnd().index());

        ExcerptAppender appender = chronicle.acquireAppender();
        UncheckedBytes<BytesStore> bytes = new UncheckedBytes<>(BytesStore.empty().bytesForRead());
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
                bytes.setBytes(bytes0);
                reader.readFrom(bytes);
            }
            long time2 = System.nanoTime() - start2;
            readHdr.sample(time2);
        }
        System.err.println("!Length is " + tailer.toEnd().index());
        if (print) {
            System.out.println("Write latencies " + writeHdr.toMicrosFormat());
            System.out.println("Read latencies " + readHdr.toMicrosFormat());
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

