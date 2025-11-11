/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue.internal.reader;

import net.openhft.chronicle.bytes.MethodId;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.reader.ChronicleHistoryReader;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class ChronicleHistoryReaderTest extends QueueTestCommon {

    @Test
    public void testWithQueueHistoryRecordHistoryInitial() {
        if (OS.isWindows())
            expectException("Read-only mode is not supported on Windows");

        checkWithQueueHistoryRecordHistoryInitial(DummyListener.class);
    }

    @Test
    public void testWithQueueHistoryRecordHistoryInitialMethodIds() {
        if (OS.isWindows())
            expectException("Read-only mode is not supported on Windows");

        checkWithQueueHistoryRecordHistoryInitial(DummyListenerId.class);
    }

    private void checkWithQueueHistoryRecordHistoryInitial(Class<? extends DummyListener> dummyClass) {
        // this is because there is no way to tell CHR to open a queue with a particular sourceId
        expectException("Overriding sourceId from existing metadata, was 0, overriding to");

        final SetTimeMessageHistory mh = new SetTimeMessageHistory();
        mh.addSourceDetails(true);
        MessageHistory.set(mh);

        int extraTiming = 1;
        File queuePath1 = IOTools.createTempFile(testName.getMethodName() + "1-");
        File queuePath2 = IOTools.createTempFile(testName.getMethodName() + "2-");
        File queuePath3 = IOTools.createTempFile(testName.getMethodName() + "3-");
        try {
            try (ChronicleQueue out = queue(queuePath1, 1)) {
                DummyListener writer = out
                        .methodWriterBuilder(dummyClass)
                        .get();
                // this will write the 1st timestamps
                writer.say("hello");
            }

            try (ChronicleQueue in = queue(queuePath1, 1);
                 ChronicleQueue out = queue(queuePath2, 2)) {
                DummyListener writer = out.methodWriterBuilder(dummyClass).get();
                final AtomicInteger numberRead = new AtomicInteger();
                // if this listener is a DummyListener then messages with methodId won't be routed to it
                DummyListenerId dummy = msg -> {
                    numberRead.incrementAndGet();
                    MessageHistory history = MessageHistory.get();
                    Assert.assertEquals(1, history.sources());
                    // written 1st then received by me
                    Assert.assertEquals(1 + extraTiming, history.timings());
                    // this writes 2 more timestamps
                    writer.say(msg);
                };
                MethodReader reader = in.createTailer().methodReader(dummy);
                assertTrue(reader.readOne());
                assertEquals("check routed to correct dest", 1, numberRead.get());
                assertFalse(reader.readOne());
            }

            try (ChronicleQueue in = queue(queuePath2, 2);
                 ChronicleQueue out = queue(queuePath3, 3)) {
                DummyListener writer = out.methodWriterBuilder(dummyClass)
                        .get();
                final AtomicInteger numberRead = new AtomicInteger();
                DummyListenerId dummy = msg -> {
                    numberRead.incrementAndGet();
                    MessageHistory history = MessageHistory.get();
                    Assert.assertEquals(2, history.sources());
                    Assert.assertEquals(3 + extraTiming, history.timings());
                    // this writes 2 more timestamps
                    writer.say(msg);
                };
                MethodReader reader = in.createTailer().methodReader(dummy);
                assertTrue(reader.readOne());
                assertEquals("check routed to correct dest", 1, numberRead.get());
                assertFalse(reader.readOne());
            }

            try (ChronicleHistoryReader chronicleHistoryReader = new ChronicleHistoryReader()
                    .withBasePath(queuePath3.toPath())
                    .withTimeUnit(TimeUnit.MICROSECONDS)
                    .withMessageSink(System.out::println)) {
                Map<String, Histogram> histos = chronicleHistoryReader.readChronicle();
                chronicleHistoryReader.outputData();

                Assert.assertEquals(5, histos.size());
                Assert.assertEquals("[1, startTo1, 2, 1to2, endToEnd]", histos.keySet().toString());
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath1.toString(), queuePath2.toString(), queuePath3.toString());
        }
    }

    @Test
    public void testPredictable() {
        runPredictable(0, null, "Timings below in MICROSECONDS\n" +
                "sourceId                   1     startTo1            2         1to2     endToEnd \n" +
                "count:                   100          100          100          100          100 \n" +
                "50:                        9           19            9           19           60 \n" +
                "90:                        9           19            9           19           60 \n" +
                "99:                        9           19            9           19           60 \n" +
                "99.9:                                                                            \n" +
                "99.99:                                                                           \n" +
                "99.999:                                                                          \n" +
                "99.9999:                                                                         \n" +
                "worst:                     9           19            9           19           60 \n");
    }

    @Test
    public void testPredictableStartIndex() {
        runPredictable(0, 33L, "Timings below in MICROSECONDS\n" +
                "sourceId                   1     startTo1            2         1to2     endToEnd \n" +
                "count:                    67           67           67           67           67 \n" +
                "50:                        9           19            9           19           60 \n" +
                "90:                        9           19            9           19           60 \n" +
                "99:                        9           19            9           19           60 \n" +
                "99.9:                                                                            \n" +
                "99.99:                                                                           \n" +
                "99.999:                                                                          \n" +
                "99.9999:                                                                         \n" +
                "worst:                     9           19            9           19           60 \n");
    }

    @Test
    public void testPredictableMeasurementWindow() {
        runPredictable(2_800, null, "Timings below in MICROSECONDS\n" +
                "sourceId                   1     startTo1            2         1to2     endToEnd \n" +
                "count:                     1            1            1            1            1 \n" +
                "50:                        9           19            9           19           60 \n" +
                "90:                        9           19            9           19           60 \n" +
                "99:                        9           19            9           19           60 \n" +
                "99.9:                                                                            \n" +
                "99.99:                                                                           \n" +
                "99.999:                                                                          \n" +
                "99.9999:                                                                         \n" +
                "worst:                     9           19            9           19           60 \n" +
                "Timings below in MICROSECONDS\n" +
                "sourceId                   1     startTo1            2         1to2     endToEnd \n" +
                "count:                    40           40           40           40           40 \n" +
                "50:                        9           19            9           19           60 \n" +
                "90:                        9           19            9           19           60 \n" +
                "99:                        9           19            9           19           60 \n" +
                "99.9:                                                                            \n" +
                "99.99:                                                                           \n" +
                "99.999:                                                                          \n" +
                "99.9999:                                                                         \n" +
                "worst:                     9           19            9           19           60 \n" +
                "Timings below in MICROSECONDS\n" +
                "sourceId                   1     startTo1            2         1to2     endToEnd \n" +
                "count:                    40           40           40           40           40 \n" +
                "50:                        9           19            9           19           60 \n" +
                "90:                        9           19            9           19           60 \n" +
                "99:                        9           19            9           19           60 \n" +
                "99.9:                                                                            \n" +
                "99.99:                                                                           \n" +
                "99.999:                                                                          \n" +
                "99.9999:                                                                         \n" +
                "worst:                     9           19            9           19           60 \n" +
                "Timings below in MICROSECONDS\n" +
                "sourceId                   1     startTo1            2         1to2     endToEnd \n" +
                "count:                    19           19           19           19           19 \n" +
                "50:                        9           19            9           19           60 \n" +
                "90:                        9           19            9           19           60 \n" +
                "99:                        9           19            9           19           60 \n" +
                "99.9:                                                                            \n" +
                "99.99:                                                                           \n" +
                "99.999:                                                                          \n" +
                "99.9999:                                                                         \n" +
                "worst:                     9           19            9           19           60 \n");
    }

    private void runPredictable(int mwMicros, Long startIndexOffset, String output) {
        // this is because there is no way to tell CHR to open a queue with a particular sourceId
        expectException("Overriding sourceId from existing metadata, was 0, overriding to");

        final SetTimeMessageHistory mh = new SetTimeMessageHistory();
        mh.addSourceDetails(true);
        MessageHistory.set(mh);

        File queuePath1 = IOTools.createTempFile(testName.getMethodName() + "1-");
        File queuePath2 = IOTools.createTempFile(testName.getMethodName() + "2-");
        File queuePath3 = IOTools.createTempFile(testName.getMethodName() + "3-");
        try {
            StringBuilder sb = new StringBuilder();
            try (ChronicleQueue q1 = queue(queuePath1, 1);
                 ChronicleQueue q2 = queue(queuePath2, 2);
                 ChronicleQueue q3 = queue(queuePath3, 3);
                 ChronicleHistoryReader chronicleHistoryReader = new ChronicleHistoryReader()
                         .withBasePath(queuePath3.toPath())
                         .withTimeUnit(TimeUnit.MICROSECONDS)
                         .withMeasurementWindow(mwMicros)
                         .withMessageSink(str -> sb.append(str).append('\n'))) {

                DummyListener writer1 = q1.methodWriterBuilder(DummyListener.class).get();
                DummyListener writer2 = q2.methodWriterBuilder(DummyListener.class).get();
                DummyListener writer3 = q3.methodWriterBuilder(DummyListener.class).get();
                MethodReader reader1 = q1.createTailer().methodReader(writer2);
                MethodReader reader2 = q2.createTailer().methodReader(writer3);

                for (int i = 0; i < 100; i++) {
                    writer1.say("hello " + i);
                    assertTrue(reader1.readOne());
                    assertTrue(reader2.readOne());
                    assertFalse(reader1.readOne());
                    assertFalse(reader2.readOne());
                }

                if (startIndexOffset != null)
                    chronicleHistoryReader.withStartIndex(startIndexOffset + q3.firstIndex());
                chronicleHistoryReader.readChronicle();
                chronicleHistoryReader.outputData();
                Assert.assertEquals(output, sb.toString());

                writer1.say("again");
                assertTrue(reader1.readOne());
                assertTrue(reader2.readOne());
                assertFalse(reader1.readOne());
                assertFalse(reader2.readOne());

                sb.setLength(0);
                chronicleHistoryReader.readChronicle();
                chronicleHistoryReader.outputData();
                Assert.assertEquals("re-reading should only show new data",
                        "Timings below in MICROSECONDS\n" +
                                "sourceId                   1     startTo1            2         1to2     endToEnd \n" +
                                "count:                     1            1            1            1            1 \n" +
                                "50:                        9           19            9           19           60 \n" +
                                "90:                        9           19            9           19           60 \n" +
                                "99:                        9           19            9           19           60 \n" +
                                "99.9:                                                                            \n" +
                                "99.99:                                                                           \n" +
                                "99.999:                                                                          \n" +
                                "99.9999:                                                                         \n" +
                                "worst:                     9           19            9           19           60 \n",
                        sb.toString());
            }
        } finally {
            IOTools.deleteDirWithFiles(queuePath1.toString(), queuePath2.toString(), queuePath3.toString());
        }
    }

    @NotNull
    private SingleChronicleQueue queue(File queuePath1, int sourceId) {
        return ChronicleQueue.singleBuilder(queuePath1).testBlockSize().sourceId(sourceId).build();
    }

    @FunctionalInterface
    interface DummyListener {
        void say(String what);
    }

    @FunctionalInterface
    interface DummyListenerId extends DummyListener {
        @Override
        @MethodId(1)
        void say(String what);
    }

    static class SetTimeMessageHistory extends VanillaMessageHistory {
        long nanoTime = 140_000_000_000_000L;

        @Override
        protected long nanoTime() {
            return nanoTime += 10_000;
        }
    }
}
