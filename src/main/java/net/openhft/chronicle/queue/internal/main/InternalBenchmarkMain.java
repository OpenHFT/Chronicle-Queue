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
    // Flags and configuration for the benchmark
    static volatile boolean running = true;
    static int throughput = Integer.getInteger("throughput", 250);  // Target throughput in MB/s
    static int runtime = Integer.getInteger("runtime", 300);  // Benchmark runtime in seconds
    static String basePath = System.getProperty("path", OS.TMP);  // Base path for the Chronicle Queue
    static volatile long readerLoopTime = 0;  // Reader loop time
    static volatile long readerEndLoopTime = 0;  // Reader end loop time
    static int counter = 0;  // Counter for iterations

    // Static block for enabling JVM safepoint logging
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
        // Print the current configuration
        System.out.println(
                "-Dthroughput=" + throughput
                        + " -Druntime=" + runtime
                        + " -Dpath=" + basePath);

        // Perform a warmup to prepare the system
        MappedFile.warmup();
        System.out.println("Warming up");
        benchmark(128);  // Run a benchmark with a 128-byte message size for warmup
        System.out.println("Warmed up");

        // Perform benchmarks with increasing message sizes, up to 16 MB
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
        Histogram writeTime = new Histogram(32, 7);  // Stores write latencies
        Histogram transportTime = new Histogram(32, 7);  // Stores transport latencies
        Histogram readTime = new Histogram(32, 7);  // Stores read latencies
        String path = basePath + "/test-q-" + messageSize;  // Path for the queue files

        // Create a new ChronicleQueue at the given path
        ChronicleQueue queue = createQueue(path);

        // Pretoucher will only work with Queue Enterprise in the path
        Thread pretoucher = new Thread(() -> {
            try (ExcerptAppender appender = queue.createAppender()) {
                Thread thread = Thread.currentThread();
                while (!thread.isInterrupted()) {
                    appender.pretouch();  // Pre-touch the queue to pre-load memory pages
                    Jvm.pause(10);  // Pause between touches
                }
            }
        });
        pretoucher.setDaemon(true);
        pretoucher.start();  // Start the pre-touching thread

        Histogram loopTime = new Histogram();  // Stores loop time measurements

        // Start a thread to read from the queue
        Thread reader = new Thread(() -> {
//            try (ChronicleQueue queue2 = createQueue(path))
            ExcerptTailer tailer = queue.createTailer().toEnd();  // Create a tailer that starts at the end of the queue
            long endLoop = System.nanoTime();
            while (running) {
                loopTime.sample((double) (System.nanoTime() - endLoop));  // Measure loop times
                Jvm.safepoint();  // Trigger JVM safepoint

//                    readerLoopTime = System.nanoTime();
//                    if (readerLoopTime - readerEndLoopTime > 1000)
//                        System.out.println("r " + (readerLoopTime - readerEndLoopTime));
//                try {
                // Perform reads from the tailer and sample latencies
                runInner(transportTime, readTime, tailer);
                runInner(transportTime, readTime, tailer);
                runInner(transportTime, readTime, tailer);
                runInner(transportTime, readTime, tailer);
//                } finally {
//                        readerEndLoopTime = System.nanoTime();
//                }
                Jvm.safepoint();  // Trigger JVM safepoint
                endLoop = System.nanoTime();  // Update the loop time for the next iteration
            }
        });
        reader.start();  // Start the reader thread
        Jvm.pause(250); // Give the reader time to start
        long next = System.nanoTime();
        long end = (long) (next + runtime * 1e9);  // End time for the benchmark

        ExcerptAppender appender = queue.createAppender();  // Create an appender to write to the queue
        while (end > System.nanoTime()) {
            long start = System.nanoTime();
            try (DocumentContext dc = appender.writingDocument(false)) {
                writeMessage(dc.wire(), messageSize);  // Write a message to the queue
            }
            long written = System.nanoTime();
            long time = written - start;  // Calculate write latency
//                System.out.println(time);
            writeTime.sample(time);  // Sample the write time

            // Ensure the reader is keeping up with the writer
            long diff = writeTime.totalCount() - readTime.totalCount();
            Thread.yield();  // Yield to give the reader time to catch up
            if (diff >= 200) {  // If the difference is too large, log details
//                long rlt = readerLoopTime;
//                long delay = System.nanoTime() - rlt;
                System.out.println("diff=" + diff /* +" delay= " + delay*/);
                StringBuilder sb = new StringBuilder();
                sb.append("Reader: profile of the thread");
                Jvm.trimStackTrace(sb, reader.getStackTrace());
                System.out.println(sb);
            }

            // Control the write throughput to match the target throughput
            next += (long) (messageSize * 1e9 / (throughput * 1e6));
            long delay = next - System.nanoTime();
            if (delay > 0)
                LockSupport.parkNanos(delay);  // Pause to maintain target throughput
        }

        // Wait for the reader to catch up before shutting down
        while (readTime.totalCount() < writeTime.totalCount())
            Jvm.pause(50);

        // Interrupt the pretoucher and reader threads and clean up
        pretoucher.interrupt();
        reader.interrupt();
        running = false;
//        monitor.interrupt();

        // Output benchmark results
        System.out.println("Loop times " + loopTime.toMicrosFormat());
        System.out.println("messageSize " + messageSize);
        System.out.println("messages " + writeTime.totalCount());
        System.out.println("write histogram: " + writeTime.toMicrosFormat());
        System.out.println("transport histogram: " + transportTime.toMicrosFormat());
        System.out.println("read histogram: " + readTime.toMicrosFormat());

        // Clean up the queue files
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
        Jvm.safepoint();  // Trigger JVM safepoint
        /*if (tailer.peekDocument()) {
            if (counter++ < 1000) {
                Jvm.safepoint();
                return;
            }
        }*/
        if (counter > 0)
            Jvm.safepoint();
        else
            Jvm.safepoint();
        counter = 0;
        try (DocumentContext dc = tailer.readingDocument(false)) {  // Read the next document
            Jvm.safepoint();
            if (!dc.isPresent()) {
                return;
            }
            long transport = System.nanoTime();  // Start measuring transport time
            Jvm.safepoint();
            Wire wire = dc.wire();
            Bytes<?> bytes = wire.bytes();
            long start = readMessage(bytes);  // Process the message from the bytes
            long end = System.nanoTime();  // End of read operation
            transportTime.sample((double) (transport - start));  // Sample transport time
            readTime.sample((double) (end - transport));  // Sample read time
        }
        Jvm.safepoint();  // Trigger JVM safepoint
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
                .blockSize(1 << 30)  // Set the block size to 1GB
                .pauserSupplier(Pauser::timedBusy)  // Use a timed busy pauser
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
        long start = bytes.readLong();  // Read the start time
        long rp = bytes.readPosition();
        long rl = bytes.readLimit();
        long addr = bytes.addressForRead(rp);
        long addrEnd = bytes.addressForRead(rl);
        Memory memory = OS.memory();
        for (addr += 8; addr + 7 < addrEnd; addr += 8)  // Read the rest of the message
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
        for (int i = 0; i < messageSize; i += 16) {  // Write the message data
            memory.writeLong(addr + i, 0L);
            memory.writeLong(addr + i + 8, 0L);
        }

        bytes.writeSkip(messageSize);
        bytes.writeLong(wp, System.nanoTime());  // Record the current time as the start time
    }
}
