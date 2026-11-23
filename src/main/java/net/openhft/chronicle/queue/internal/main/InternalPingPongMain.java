/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.main;

import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.BackgroundResourceReleaser;
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
    //    static int throughput = Integer.getInteger("throughput", 250); // MB/s
    static int runtime = Integer.getInteger("runtime", 30); // seconds
    static String basePath = System.getProperty("path", OS.TMP);
    static AtomicLong writeTime = new AtomicLong();
    static AtomicInteger writeCount = new AtomicInteger();
    static AtomicInteger readCount = new AtomicInteger();
    static AtomicBoolean running = new AtomicBoolean(true);

    static {
        System.setProperty("jvm.safepoint.enabled", "true");
    }

    /**
     * Main method to run the ping-pong benchmark.
     *
     * @param args Command-line arguments (not used in this benchmark)
     */
    public static void main(String[] args) {
        System.out.println(
//                "-Dthroughput=" + throughput
                " -Druntime=" + runtime
                        + " -Dpath=" + basePath);
        MappedFile.warmup();

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
        Histogram readDelay = new Histogram();
        Histogram readDelay2 = new Histogram();
        try (ChronicleQueue queue = createQueue(path)) {
            runPingPong(queue, size, readDelay, readDelay2);
        }
        System.out.println("read delay: " + readDelay.toMicrosFormat());
        System.out.println("read delay2: " + readDelay2.toMicrosFormat());
        // Closing Queue can defer unmapping; Windows cannot delete mapped roll files.
        BackgroundResourceReleaser.releasePendingResources();
        IOTools.deleteDirWithFiles(path, 2);
    }

    private static void runPingPong(ChronicleQueue queue, int size, Histogram readDelay, Histogram readDelay2) {
        Thread reader = new Thread(() -> {
            try (ExcerptTailer tailer = queue.createTailer()) {
                while (running.get()) {
                    //noinspection StatementWithEmptyBody
                    while (running.get() && readCount.get() == writeCount.get()) ;

                    long wakeTime = System.nanoTime();
                    while (true) {
                        if (!running.get())
                            return;
                        try (DocumentContext dc = tailer.readingDocument(true)) {
                            if (!dc.isPresent())
                                continue;
                        }
                        break;
                    }
                    final long delay = wakeTime - writeTime.get();
                    final long time = System.nanoTime() - wakeTime;
                    readDelay2.sample(time);
                    readDelay.sample(delay);
                    if (time + delay > 20_000)
                        System.out.println("td " + delay + " + " + time);
                    if (readCount.get() == 100000) {
                        System.out.println("reset");
                        readDelay.reset();
                        readDelay2.reset();
                    }
                    readCount.incrementAndGet();
                }
            }
        });
        reader.setDaemon(true);
        reader.start();
        try (ExcerptAppender appender = queue.createAppender()) {
            Jvm.pause(100);

            final long finish = System.currentTimeMillis() + runtime * 1000L;
            while (System.currentTimeMillis() < finish) {
                if (readCount.get() < writeCount.get()) {
                    Thread.yield();
                    continue;
                }
                try (DocumentContext dc = appender.writingDocument(false)) {
                    dc.wire().bytes().writeSkip(size);
                }
                writeCount.incrementAndGet();
                writeTime.set(System.nanoTime());
            }
        } finally {
            stopReader(reader);
        }
    }

    // Restore the interrupt after joining so that a retry can wait for the reader.
    @SuppressWarnings("java:S2142")
    static void stopReader(Thread reader) {
        running.set(false);
        boolean interrupted = false;
        while (reader.isAlive()) {
            try {
                reader.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Creates a Chronicle Queue at the given path.
     *
     * @param path The path where the queue will be created
     * @return The created ChronicleQueue instance
     */
    @NotNull
    private static ChronicleQueue createQueue(String path) {
        return ChronicleQueue.single(path);
    }
}
