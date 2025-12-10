/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.UncheckedBytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.queue.QueuePerfTestSupport.TestReader;
import net.openhft.chronicle.queue.QueuePerfTestSupport.TestWriter;
import org.junit.Test;

import java.io.IOException;

import static net.openhft.chronicle.queue.TestFacadeInterfaces.IFacade;
import static net.openhft.chronicle.queue.QueuePerfTestSupport.readMany;
import static net.openhft.chronicle.queue.QueuePerfTestSupport.writeMany;
import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"rawtypes", "unchecked"})
public class SingleChroniclePerfMainTest extends QueueTestCommon {
    private static final int count = 1_000_000;
    private static final int size = 4 << 10;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
    }

    public static void main(String[] args) throws IOException {
        for (int t = 0; t < 2; t++) {
            doPerfTest(
                    bytes -> writeMany(bytes, size),
                    bytes -> readMany(bytes, size),
                    t == 0 ? 100_000 : count, t > 0);
        }
    }

    private static void doPerfTest(TestWriter<Bytes<?>> writer, TestReader<Bytes<?>> reader, int count, boolean print) {
        Histogram writeHdr = new Histogram(30, 7);
        Histogram readHdr = new Histogram(30, 7);
        String file = OS.getTarget() + "/deleteme-" + Time.uniqueId();
        try (ChronicleQueue chronicle = single(file).blockSize(64 << 20).build();
             ExcerptAppender appender = chronicle.createAppender()) {
            UncheckedBytes bytes = new UncheckedBytes(BytesStore.empty().bytesForRead());
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
        }
        if (print) {
            System.out.println("Write latencies " + writeHdr.toMicrosFormat());
            System.out.println("Read latencies " + readHdr.toMicrosFormat());
        }
        IOTools.deleteDirWithFiles(file, 3);
    }

    @Test
    public void testFacade() {
        IFacade f = Values.newNativeReference(IFacade.class);
        Byteable byteable = (Byteable) f;
        long capacity = byteable.maxSize();
        BytesStore<?, Void> bytesStore = BytesStore.nativeStore(capacity);
        byteable.bytesStore(bytesStore, 0, capacity);
        assertEquals(bytesStore, byteable.bytesStore());
        assertEquals(0, byteable.offset());
        bytesStore.releaseLast();
    }
}
