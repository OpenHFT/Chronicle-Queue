/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue.reader;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.VanillaMessageHistory;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
public class ChronicleHistoryReaderTest {

    @Test
    public void testWithQueueHistoryRecordHistoryInitial() {
        Assume.assumeFalse(OS.isWindows());
        doTest(true);
    }

    @Test
    public void testWithQueueHistory() {
        Assume.assumeFalse(OS.isWindows());
        doTest(false);
    }

    private void doTest(boolean recordHistoryFirst) {
        VanillaMessageHistory veh = new VanillaMessageHistory();
        veh.addSourceDetails(true);
        MessageHistory.set(veh);

        int extraTiming = recordHistoryFirst ? 1 : 0;
        long nanoTime = System.nanoTime();
        File queuePath = new File(OS.TARGET, "testWithQueueHistory-" + nanoTime);
        File queuePath2 = new File(OS.TARGET, "testWithQueueHistory2-" + nanoTime);
        File queuePath3 = new File(OS.TARGET, "testWithQueueHistory3-" + nanoTime);
        try {
            try (ChronicleQueue out = ChronicleQueue.singleBuilder(queuePath).testBlockSize().sourceId(1).build()) {
                DummyListener writer = out.acquireAppender()
                        .methodWriterBuilder(DummyListener.class)
                        .recordHistory(recordHistoryFirst)
                        .get();
                writer.say("hello");
            }

            try (ChronicleQueue in = ChronicleQueue.singleBuilder(queuePath).testBlockSize().sourceId(1).build();
                 ChronicleQueue out = ChronicleQueue.singleBuilder(queuePath2).testBlockSize().build()) {
                DummyListener writer = out.acquireAppender()
                        .methodWriterBuilder(DummyListener.class)
                        .recordHistory(true)
                        .get();
                DummyListener dummy = msg -> {
                    MessageHistory history = MessageHistory.get();
                    Assert.assertEquals(1, history.sources());
                    // written 1st then received by me
                    Assert.assertEquals(1 + extraTiming, history.timings());
                    writer.say(msg);
                };
                MethodReader reader = in.createTailer().methodReader(dummy);
                assertTrue(reader.readOne());
                assertFalse(reader.readOne());
            }

            try (ChronicleQueue in = ChronicleQueue.singleBuilder(queuePath2).testBlockSize().sourceId(2).build();
                 ChronicleQueue out = ChronicleQueue.singleBuilder(queuePath3).testBlockSize().build()) {
                DummyListener writer = out.acquireAppender()
                        .methodWriterBuilder(DummyListener.class)
                        .recordHistory(true)
                        .get();
                DummyListener dummy = msg -> {
                    MessageHistory history = MessageHistory.get();
                    Assert.assertEquals(2, history.sources());
                    Assert.assertEquals(3 + extraTiming, history.timings());
                    writer.say(msg);
                };
                MethodReader reader = in.createTailer().methodReader(dummy);
                assertTrue(reader.readOne());
                assertFalse(reader.readOne());
            }

            ChronicleHistoryReader chronicleHistoryReader = new ChronicleHistoryReader().
                    withBasePath(queuePath3.toPath()).
                    withTimeUnit(TimeUnit.MICROSECONDS);
            Map<String, Histogram> histos = chronicleHistoryReader.readChronicle();

            chronicleHistoryReader.withMessageSink(System.out::println);
            chronicleHistoryReader.outputData();

            if (recordHistoryFirst) {
                Assert.assertEquals(5, histos.size());
                Assert.assertEquals("[1, startTo1, 2, 1to2, endToEnd]", histos.keySet().toString());

            } else {
                Assert.assertEquals(4, histos.size());
                Assert.assertEquals("[1, 2, 1to2, endToEnd]", histos.keySet().toString());
            }
        } finally {
            try {
                IOTools.shallowDeleteDirWithFiles(queuePath);
                IOTools.shallowDeleteDirWithFiles(queuePath2);
                IOTools.shallowDeleteDirWithFiles(queuePath3);
            } catch (Exception e) {
            }
        }
    }

    @FunctionalInterface
    private interface DummyListener {
        void say(String what);
    }
}