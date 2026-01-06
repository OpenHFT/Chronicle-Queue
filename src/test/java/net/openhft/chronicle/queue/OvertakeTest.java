/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.DocumentContext;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Index runs away on double close when tailer reuses the document context.
 */
@RequiredForClient
public class OvertakeTest extends QueueTestCommon {

    private String path;

    private long appendedIndex;

    private final int messages = 500;

    private static long doReadBad(@NotNull ExcerptTailer tailer, int expected, boolean additionalClose) {
        int[] i = {0};
        long tailIndex = 0;
        while (true) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (!dc.isPresent())
                    break;
                tailIndex = tailer.index();

                dc.wire().read("log").marshallable(m -> {
                    String msg = m.read("msg").text();
                    assertNotNull(msg, "message text should not be null when reading document from queue");
                    i[0]++;
                });
                if (additionalClose) {
                    Closeable.closeQuietly(dc);
                }
            }
        }
        assertEquals(expected, i[0], "tailer should read exactly " + expected + " messages from queue");
        return tailIndex;
    }

    @BeforeEach
    public void before() {
        path = OS.getTarget() + "/" + getClass().getSimpleName() + "-" + Time.uniqueId();
        try (ChronicleQueue appender_queue = ChronicleQueue.singleBuilder(path)
                .testBlockSize()
                .writeBufferMode(BufferMode.None)
                .build();
             ExcerptAppender appender = appender_queue.createAppender()) {
            for (int i = 0; i < messages; i++) {
                final long l = i;
                appender.writeDocument(wireOut -> wireOut.write("log").marshallable(m -> {
                            m.write("msg").text("hello world ola multi-verse");
                            m.write("ts").int64(l);
                        }
                ));
            }
            appendedIndex = appender.lastIndexAppended();
        }
    }

    @Override
    @BeforeEach
    public void threadDump() {
        super.threadDump();
    }

    @Test
    @DisplayName("Append and tail with and without additional close")
    public void appendAndTail() {
        try (ChronicleQueue tailer_queue = ChronicleQueue.singleBuilder(path)
                .testBlockSize()
                .writeBufferMode(BufferMode.None)
                .build()) {
            ExcerptTailer tailer = tailer_queue.createTailer();
            tailer = tailer.toStart();
            long tailIndex;
            tailIndex = doReadBad(tailer, messages, false);
            assertEquals(appendedIndex, tailIndex, "tailer should reach index of last appended message after reading all messages");
            tailer = tailer_queue.createTailer();
            tailer = tailer.toStart();
            tailIndex = doReadBad(tailer, messages, true);
            assertEquals(appendedIndex, tailIndex, "tailer should reach index of last appended message after reading all messages with additional close");
        }
    }

    @Override
    public void tearDown() {
        try {
            IOTools.deleteDirWithFiles(path, 2);
        } catch (Exception e) {
            Jvm.warn().on(OvertakeTest.class, "Failed to delete queue directory " + path, e);
        }
    }

    @Test
    @DisplayName("Tailer thread reaches writer index during concurrent reads")
    public void threadingTest() throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService execService = Executors.newFixedThreadPool(2,
                new NamedThreadFactory("test"));
        SynchronousQueue<Long> sync = new SynchronousQueue<>();
        long tailIndex;

        MyAppender myapp = new MyAppender(sync);
        Future<Long> f = execService.submit(myapp);
        try (ChronicleQueue tailer_queue = ChronicleQueue.singleBuilder(path)
                .testBlockSize()
                .writeBufferMode(BufferMode.None)
                .build()) {
            tailIndex = 0;
            MyTailer mytailer = new MyTailer(tailer_queue, tailIndex, sync);
            Future<Long> f2 = execService.submit(mytailer);
            tailIndex = f2.get(10, TimeUnit.SECONDS);
            appendedIndex = f.get(10, TimeUnit.SECONDS);
            assertEquals(appendedIndex, tailIndex, "tailer thread should reach index of last message appended by writer thread in concurrent scenario");
        }
        execService.shutdown();
        execService.awaitTermination(1, TimeUnit.SECONDS);
    }

    class MyAppender implements Callable<Long> {
        final SynchronousQueue<Long> sync;

        MyAppender(SynchronousQueue<Long> sync) {
            this.sync = sync;
        }

        @Override
        public Long call() throws InterruptedException {
            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(path)
                    //.testBlockSize()
                    //.rollCycle(TEST_DAILY)
                    .writeBufferMode(BufferMode.None)
                    .build();
                 ExcerptAppender appender = queue.createAppender()) {
                for (int i = 0; i < 50; i++) {
                    appender.writeDocument(wireOut -> wireOut.write("log").marshallable(m ->
                            m.write("msg").text("hello world2 ")));
                }
                long index = appender.lastIndexAppended();
                sync.put(index);
                Long fromReader = sync.take();
                if (index != fromReader) {
                    Jvm.warn().on(MyAppender.class, "Writer index mismatch " + index + " vs. " + fromReader);
                }
                for (int i = 0; i < 50; i++) {
                    appender.writeDocument(wireOut -> wireOut.write("log").marshallable(m ->
                            m.write("msg").text("hello world2 ")));
                }
                index = appender.lastIndexAppended();
                sync.put(index);
                return index;
            }
        }
    }

    class MyTailer implements Callable<Long> {

        final ChronicleQueue queue;
        final long startIndex;
        final SynchronousQueue<Long> sync;

        MyTailer(ChronicleQueue q, long s, SynchronousQueue<Long> sync) {
            queue = q;
            startIndex = s;
            this.sync = sync;
        }

        @Override
        public Long call() throws InterruptedException {
            ExcerptTailer tailer = queue.createTailer();
            tailer.moveToIndex(startIndex);
            long fromWriter = sync.take();
            long index = doReadBad(tailer, messages + 50, false);
            if (index != fromWriter) {
                Jvm.warn().on(MyTailer.class, "Reader phase1 mismatch " + index + " vs. " + fromWriter);
            }
            sync.put(index);
            fromWriter = sync.take();
            index = doReadBad(tailer, 50, false);
            if (index != fromWriter) {
                Jvm.warn().on(MyTailer.class, "Reader phase2 mismatch " + index + " vs. " + fromWriter);
            }
            return index;
        }
    }
}
