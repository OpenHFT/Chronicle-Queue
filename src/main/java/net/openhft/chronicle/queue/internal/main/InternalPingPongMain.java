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

import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The InternalPingPongMain class demonstrates a ping-pong benchmark using Chronicle Queue.
 * It writes messages to the queue and reads them back in another thread, recording the latencies
 * for both operations. The benchmark runs for a defined duration, and results are printed at the end.
 */
public final class InternalPingPongMain {

    // Runtime duration in seconds, default is 30 seconds if not specified via system properties
    static int runtime = Integer.getInteger("runtime", 30);
    // Base directory path for the queue files, defaults to system temporary directory if not set
    static String basePath = System.getProperty("path", OS.TMP);
    // Atomic variables to track write timestamps and counts
    static AtomicLong writeTime = new AtomicLong();
    static AtomicInteger writeCount = new AtomicInteger();
    static AtomicInteger readCount = new AtomicInteger();
    // Atomic flag to indicate whether the benchmark is still running
    static AtomicBoolean running = new AtomicBoolean(true);

    static {
        // Enable JVM safepoint tracking for latency measurement accuracy
        System.setProperty("jvm.safepoint.enabled", "true");
    }

    /**
     * Main method to run the ping-pong benchmark.
     *
     * @param args Command-line arguments (not used in this benchmark)
     */
    public static void main(String[] args) {
        System.out.println(" -Druntime=" + runtime + " -Dpath=" + basePath);
        MappedFile.warmup();  // Warm up the MappedFile system for consistent performance

        // Run the ping-pong benchmark with message size of 64 bytes
        pingPong(64);
    }

    /**
     * Executes the ping-pong benchmark, where one thread writes to a Chronicle Queue
     * and another thread reads from it, measuring the time between writes and reads.
     *
     * @param size The size of the message to write in bytes
     */
    static void pingPong(int size) {
        String path = InternalPingPongMain.basePath + "/test-q-" + Time.uniqueId();
        Histogram readDelay = new Histogram();  // Histogram for read delays
        Histogram readDelay2 = new Histogram();  // Another histogram for additional read latency analysis

        try (ChronicleQueue queue = createQueue(path)) {

            // Thread responsible for reading from the queue
            Thread reader = new Thread(() -> {
                ExcerptTailer tailer = queue.createTailer();
                while (running.get()) {
                    // Wait until there's a message to read
                    while (readCount.get() == writeCount.get()) ;

                    long wakeTime = System.nanoTime();  // Record the time we started reading
                    while (running.get()) {
                        try (DocumentContext dc = tailer.readingDocument(true)) {
                            if (!dc.isPresent())
                                continue;  // Skip if there's no document present
                        }
                        break;
                    }
                    // Measure the time between when the write happened and the read started
                    final long delay = wakeTime - writeTime.get();
                    final long time = System.nanoTime() - wakeTime;
                    readDelay2.sample(time);  // Record the time it took to read the message
                    readDelay.sample(delay);  // Record the delay before the read started
                    if (time + delay > 20_000)
                        System.out.println("td " + delay + " + " + time);

                    if (readCount.get() == 100000) {  // Reset histograms after a certain number of reads
                        System.out.println("reset");
                        readDelay.reset();
                        readDelay2.reset();
                    }
                    readCount.incrementAndGet();  // Increment the read count
                }
            });
            reader.setDaemon(true);  // Mark the reader thread as a daemon
            reader.start();  // Start the reader thread
            Jvm.pause(100);  // Pause to allow the reader to start up

            // Calculate the finish time based on the runtime property
            final long finish = System.currentTimeMillis() + runtime * 1000L;
            final ExcerptAppender appender = queue.createAppender();  // Create an appender to write to the queue

            // Write messages to the queue until the runtime limit is reached
            while (System.currentTimeMillis() < finish) {
                if (readCount.get() < writeCount.get()) {  // Wait for the reader to catch up if necessary
                    Thread.yield();
                    continue;
                }
                try (DocumentContext dc = appender.writingDocument(false)) {
                    dc.wire().bytes().writeSkip(size);  // Write a message of the specified size
                }
                writeCount.incrementAndGet();  // Increment the write count
                writeTime.set(System.nanoTime());  // Record the time the write occurred
            }
            running.set(false);  // Stop the benchmark

        }

        // Output the histograms for the read delays
        System.out.println("read delay: " + readDelay.toMicrosFormat());
        System.out.println("read delay2: " + readDelay2.toMicrosFormat());

        // Clean up the queue files after the benchmark is finished
        IOTools.deleteDirWithFiles(path, 2);
    }

    /**
     * Creates a Chronicle Queue at the given path.
     *
     * @param path The path where the queue will be created
     * @return The created ChronicleQueue instance
     */
    @NotNull
    private static ChronicleQueue createQueue(String path) {
        return ChronicleQueue.single(path);  // Create a single ChronicleQueue instance at the given path
    }
}
