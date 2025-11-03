/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Memory;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.LockSupport;

/**
 * Internal benchmark utility for testing Chronicle Queue throughput.
 * <p>
 * The benchmark can be configured via system properties:
 * <ul>
 *     <li>{@code throughput} - the target throughput in MB/s (default 250)</li>
 *     <li>{@code runtime} - the benchmark runtime in seconds (default 300)</li>
 *     <li>{@code path} - the base path for Chronicle Queue (default OS temp directory)</li>
 * </ul>
 */
public class InternalBenchmarkMain {
    static volatile boolean running = true;
    static int throughput = Integer.getInteger("throughput", 250); // MB/s
    static int runtime = Integer.getInteger("runtime", 300); // seconds
    static String basePath = System.getProperty("path", OS.TMP);
    static volatile long readerLoopTime = 0;
    static volatile long readerEndLoopTime = 0;
    static int counter = 0;

    static {
        System.setProperty("jvm.safepoint.enabled", "true");
    }

    /**
     * The main method executes the benchmark. The throughput, runtime, and base path can be
     * configured using system properties.
     *
     * @param args Command-line arguments (not used)
     */
    public static void main(String[] args) {
        System.out.println(
                "-Dthroughput=" + throughput
                        + " -Druntime=" + runtime
                        + " -Dpath=" + basePath);
        MappedFile.warmup();
        System.out.println("Warming up");
        benchmark(128);
        System.out.println("Warmed up");
        for (int size = 64; size <= 16 << 20; size *= 4) {
            benchmark(size);
        }
    }

    /**
     * Runs the benchmark for a specified message size.
     * Measures write, transport, and read latencies, and controls the
     * flow of writing and reading through ChronicleQueue.
     *
     * @param messageSize the size of each message in bytes
     */
    static void benchmark(int messageSize) {
        Histogram writeTime = new Histogram(32, 7);
        Histogram transportTime = new Histogram(32, 7);
        Histogram readTime = new Histogram(32, 7);
        String path = basePath + "/test-q-" + messageSize;

        ChronicleQueue queue = createQueue(path);

        // Pretoucher will only work with Queue Enterprise in the path
        Thread pretoucher = new Thread(() -> {
            try (ExcerptAppender appender = queue.createAppender()) {
                Thread thread = Thread.currentThread();
                while (!thread.isInterrupted()) {
                    appender.pretouch();
                    Jvm.pause(10);
                }
            }
        });
        pretoucher.setDaemon(true);
        pretoucher.start();

        Histogram loopTime = new Histogram();

        Thread reader = new Thread(() -> {
//            try (ChronicleQueue queue2 = createQueue(path))
            ExcerptTailer tailer = queue.createTailer().toEnd();
            long endLoop = System.nanoTime();
            while (running) {
                loopTime.sample((double) (System.nanoTime() - endLoop));
                Jvm.safepoint();

//                    readerLoopTime = System.nanoTime();
//                    if (readerLoopTime - readerEndLoopTime > 1000)
//                        System.out.println("r " + (readerLoopTime - readerEndLoopTime));
//                try {
                runInner(transportTime, readTime, tailer);
                runInner(transportTime, readTime, tailer);
                runInner(transportTime, readTime, tailer);
                runInner(transportTime, readTime, tailer);
//                } finally {
//                        readerEndLoopTime = System.nanoTime();
//                }
                Jvm.safepoint();
                endLoop = System.nanoTime();
            }
        });
        reader.start();
        Jvm.pause(250); // give the reader time to start
        long next = System.nanoTime();
        long end = (long) (next + runtime * 1e9);

        ExcerptAppender appender = queue.createAppender(); // NOSONAR
        while (end > System.nanoTime()) {
            long start = System.nanoTime();
            try (DocumentContext dc = appender.writingDocument(false)) {
                writeMessage(dc.wire(), messageSize);
            }
            long written = System.nanoTime();
            long time = written - start;
//                System.out.println(time);
            writeTime.sample(time);

            long diff = writeTime.totalCount() - readTime.totalCount();
            Thread.yield();
            if (diff >= 200) {
//                long rlt = readerLoopTime;
//                long delay = System.nanoTime() - rlt;
                System.out.println("diff=" + diff /* +" delay= " + delay*/);
                StringBuilder sb = new StringBuilder();
                sb.append("Reader: profile of the thread");
                Jvm.trimStackTrace(sb, reader.getStackTrace());
                System.out.println(sb);
            }

            next += (long) (messageSize * 1e9 / (throughput * 1e6));
            long delay = next - System.nanoTime();
            if (delay > 0)
                LockSupport.parkNanos(delay);
        }

        while (readTime.totalCount() < writeTime.totalCount())
            Jvm.pause(50);

        pretoucher.interrupt();
        reader.interrupt();
        running = false;
//        monitor.interrupt();

        System.out.println("Loop times " + loopTime.toMicrosFormat());
        System.out.println("messageSize " + messageSize);
        System.out.println("messages " + writeTime.totalCount());
        System.out.println("write histogram: " + writeTime.toMicrosFormat());
        System.out.println("transport histogram: " + transportTime.toMicrosFormat());
        System.out.println("read histogram: " + readTime.toMicrosFormat());
        IOTools.deleteDirWithFiles(path, 2);
        Jvm.pause(1000);
    }

    /**
     * Processes a single document from the queue using the provided tailer and samples transport and read times.
     *
     * @param transportTime The histogram for measuring transport times
     * @param readTime      The histogram for measuring read times
     * @param tailer        The ExcerptTailer used to read from the queue
     */
    private static void runInner(Histogram transportTime, Histogram readTime, ExcerptTailer tailer) {
        Jvm.safepoint();
        /*if (tailer.peekDocument()) {
            if (counter++ < 1000) {
                Jvm.safepoint();
                return;
            }
        }*/
        Jvm.safepoint();
        counter = 0;
        try (DocumentContext dc = tailer.readingDocument(false)) {
            Jvm.safepoint();
            if (!dc.isPresent()) {
                return;
            }
            long transport = System.nanoTime();
            Jvm.safepoint();
            Wire wire = dc.wire();
            Bytes<?> bytes = wire.bytes();
            long start = readMessage(bytes);
            long end = System.nanoTime();
            transportTime.sample((double) (transport - start));
            readTime.sample((double) (end - transport));
        }
        Jvm.safepoint();
    }

    /**
     * Creates a new ChronicleQueue with the given path.
     *
     * @param path The path for the Chronicle Queue
     * @return A new ChronicleQueue instance
     */
    @NotNull
    private static ChronicleQueue createQueue(String path) {
        return ChronicleQueue.singleBuilder(path)
                .blockSize(1 << 30)
                .pauserSupplier(Pauser::timedBusy)
                .build();
    }

    /**
     * Reads a message from the provided bytes and returns the start time of the message.
     *
     * @param bytes The bytes containing the message
     * @return The start time of the message
     */
    private static long readMessage(Bytes<?> bytes) {
        Jvm.safepoint();
        long start = bytes.readLong();
        long rp = bytes.readPosition();
        long rl = bytes.readLimit();
        long addr = bytes.addressForRead(rp);
        long addrEnd = bytes.addressForRead(rl);
        Memory memory = OS.memory();
        for (addr += 8; addr + 7 < addrEnd; addr += 8)
            memory.readLong(addr);
        Jvm.safepoint();
        return start;
    }

    /**
     * Writes a message of the specified size to the given wire.
     *
     * @param wire        The wire to write to
     * @param messageSize The size of the message to write
     */
    private static void writeMessage(Wire wire, int messageSize) {
        Bytes<?> bytes = wire.bytes();
        long wp = bytes.writePosition();
        long addr = bytes.addressForWrite(wp);
        Memory memory = OS.memory();
        for (int i = 0; i < messageSize; i += 16) {
            memory.writeLong(addr + i, 0L);
            memory.writeLong(addr + i + 8, 0L);
        }

        bytes.writeSkip(messageSize);
        bytes.writeLong(wp, System.nanoTime());
    }
}
