/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.jitter;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedFile;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

public class QueueWriteJitterMain {
    private static final String PROFILE_OF_THE_THREAD = "profile of the thread";

    private static int runTime = Integer.getInteger("runTime", 600); // seconds
    private static int size = Integer.getInteger("size", 128); // bytes
    private static int sampleTime = Integer.getInteger("sampleTime", 30); // micro-seconds
    private static volatile boolean running = true;
    private static volatile long writeStarted = Long.MAX_VALUE;

    static {
        System.setProperty("jvm.safepoint.enabled", "true");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
    }

    public static void main(String[] args) {
        new QueueWriteJitterMain().run();
    }

    public void run() {
        MappedFile.warmup();

        String path = "test-q-" + Time.uniqueId();
        System.out.println("Writing to " + path);

        Thread pretoucher = new Thread(() -> {
            try (ChronicleQueue q = createQueue(path);
                 ExcerptAppender appender = q.createAppender()) {
                while (true) {
                    Thread.sleep(50);
                    appender.pretouch();
                }
            } catch (InterruptedException ie) {
                if (running)
                    ie.printStackTrace();
            }
        });
        pretoucher.setDaemon(true);
        pretoucher.start();

        Thread writer = new Thread(() -> {
            try (ChronicleQueue q = createQueue(path);
                 ExcerptAppender appender = q.createAppender()) {
                while (running) {
                    writeStarted = System.nanoTime();
                    Jvm.safepoint();
                    try (DocumentContext dc = appender.writingDocument(false)) {
                        Jvm.safepoint();
                        Bytes<?> bytes = dc.wire().bytes();
                        for (int i = 0; i < size; i += 8)
                            bytes.writeLong(i);
                        Jvm.safepoint();
                    }
                    Jvm.safepoint();
                    writeStarted = Long.MAX_VALUE;
                    waitForNext(Math.min(100, sampleTime));
                }
            }
        });
        writer.setDaemon(true);
        writer.start();
        Jvm.pause(100); // give it time to start

        try (ChronicleQueue q = createQueue(path)) {
            ExcerptTailer tailer = q.createTailer();
            long start0 = System.currentTimeMillis();
            do {
                if (writeStarted < Long.MAX_VALUE) {
                    // overflow exists loop
                    while (writeStarted + sampleTime * 1000 > System.nanoTime())
                        Thread.yield();

                    if (writeStarted < Long.MAX_VALUE) {

                        StackTraceElement[] stes = writer.getStackTrace();
                        if (!stes[1].getMethodName().equals("waitForNext")) {
                            StringBuilder sb = new StringBuilder();
                            sb.append(PROFILE_OF_THE_THREAD);
                            Jvm.trimStackTrace(sb, stes);
                            System.out.println(sb);
                        }
                    }
                }
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        waitForNext(Math.min(100, sampleTime));
                }
                Thread.yield();

            } while (System.currentTimeMillis() < start0 + runTime * 1_000);
        }
        running = false;
        pretoucher.interrupt();

        IOTools.deleteDirWithFiles(path, 2);
    }

    private void waitForNext(int sampleTime) {
        long start1 = System.nanoTime();
        do {
            Thread.yield();
        } while (System.nanoTime() < start1 + sampleTime * 1000);
        long time1 = System.nanoTime() - start1;
        if (time1 > sampleTime * 1000 * 10) {
            System.out.println("Time paused = " + time1 / 1000 + " us");
        }
    }

    private ChronicleQueue createQueue(String path) {
        return SingleChronicleQueueBuilder.single(path).testBlockSize().build();
    }
}
