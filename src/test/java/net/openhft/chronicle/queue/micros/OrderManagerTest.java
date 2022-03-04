/*
 * Copyright 2016-2020 chronicle.software
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

package net.openhft.chronicle.queue.micros;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.testframework.FlakyTestRunner;
import net.openhft.chronicle.wire.MessageHistory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

public class OrderManagerTest extends QueueTestCommon {

    @Test
    public void testOnOrderIdea() {
        // what we expect to happen
        OrderListener listener = createMock(OrderListener.class);
        listener.onOrder(new Order("EURUSD", Side.Buy, 1.1167, 1_000_000));
        replay(listener);

        // build our scenario
        OrderManager orderManager = new OrderManager(listener);
        SidedMarketDataCombiner combiner = new SidedMarketDataCombiner(orderManager);

        // events in
        orderManager.onOrderIdea(new OrderIdea("EURUSD", Side.Buy, 1.1180, 2e6)); // not expected to trigger

        combiner.onSidedPrice(new SidedPrice("EURUSD", 123456789000L, Side.Sell, 1.1172, 2e6));
        combiner.onSidedPrice(new SidedPrice("EURUSD", 123456789100L, Side.Buy, 1.1160, 2e6));

        combiner.onSidedPrice(new SidedPrice("EURUSD", 123456789100L, Side.Buy, 1.1167, 2e6));

        orderManager.onOrderIdea(new OrderIdea("EURUSD", Side.Buy, 1.1165, 1e6)); // expected to trigger

        verify(listener);
    }

    @Test
    public void testWithQueue() {
        File queuePath = new File(OS.getTarget(), "testWithQueue-" + Time.uniqueId());
        try {
            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(queuePath).testBlockSize().build()) {
                OrderIdeaListener orderManager = queue.acquireAppender().methodWriter(OrderIdeaListener.class, MarketDataListener.class);
                SidedMarketDataCombiner combiner = new SidedMarketDataCombiner((MarketDataListener) orderManager);

                // events in
                orderManager.onOrderIdea(new OrderIdea("EURUSD", Side.Buy, 1.1180, 2e6)); // not expected to trigger

                combiner.onSidedPrice(new SidedPrice("EURUSD", 123456789000L, Side.Sell, 1.1172, 2e6));
                combiner.onSidedPrice(new SidedPrice("EURUSD", 123456789100L, Side.Buy, 1.1160, 2e6));

                combiner.onSidedPrice(new SidedPrice("EURUSD", 123456789100L, Side.Buy, 1.1167, 2e6));

                orderManager.onOrderIdea(new OrderIdea("EURUSD", Side.Buy, 1.1165, 1e6)); // expected to trigger
            }

// what we expect to happen
            OrderListener listener = createMock(OrderListener.class);
            listener.onOrder(new Order("EURUSD", Side.Buy, 1.1167, 1_000_000));
            replay(listener);

            try (ChronicleQueue queue = ChronicleQueue.singleBuilder(queuePath).testBlockSize().build()) {
                // build our scenario
                OrderManager orderManager = new OrderManager(listener);
                MethodReader reader = queue.createTailer().methodReader(orderManager);
                for (int i = 0; i < 5; i++)
                    assertTrue(reader.readOne());

                assertFalse(reader.readOne());
                // System.out.println(queue.dump());
            }

            verify(listener);
        } finally {
            try {
                IOTools.shallowDeleteDirWithFiles(queuePath);
            } catch (Exception e) {
            }
        }
    }

    @Test
    @Ignore("TODO FIX")
    public void testWithQueueHistory() throws Throwable {
        FlakyTestRunner.builder(this::testWithQueueHistory0).build().run();
    }

    public void testWithQueueHistory0() {
        File queuePath = new File(OS.getTarget(), "testWithQueueHistory-" + Time.uniqueId());
        File queuePath2 = new File(OS.getTarget(), "testWithQueueHistory-down-" + Time.uniqueId());
        try {
            try (ChronicleQueue out = ChronicleQueue.singleBuilder(queuePath)
                    .testBlockSize()
                    .sourceId(1)
                    .build()) {
                OrderIdeaListener orderManager = out.acquireAppender()
                        .methodWriterBuilder(OrderIdeaListener.class)
                        .addInterface(MarketDataListener.class)
                        .get();
                SidedMarketDataCombiner combiner = new SidedMarketDataCombiner((MarketDataListener) orderManager);

                // events in
                orderManager.onOrderIdea(new OrderIdea("EURUSD", Side.Buy, 1.1180, 2e6)); // not expected to trigger

                combiner.onSidedPrice(new SidedPrice("EURUSD", 123456789000L, Side.Sell, 1.1172, 2e6));
                combiner.onSidedPrice(new SidedPrice("EURUSD", 123456789100L, Side.Buy, 1.1160, 2e6));

                combiner.onSidedPrice(new SidedPrice("EURUSD", 123456789100L, Side.Buy, 1.1167, 2e6));

                orderManager.onOrderIdea(new OrderIdea("EURUSD", Side.Buy, 1.1165, 1e6)); // expected to trigger
            }

            try (ChronicleQueue in = ChronicleQueue.singleBuilder(queuePath)
                    .testBlockSize()
                    .sourceId(1)
                    .build();
                 ChronicleQueue out = ChronicleQueue.singleBuilder(queuePath2)
                         .testBlockSize()
                         .sourceId(2)
                         .build()) {

                OrderListener listener = out.acquireAppender()
                        .methodWriterBuilder(OrderListener.class)
                        .get();
                // build our scenario
                OrderManager orderManager = new OrderManager(listener);
                MethodReader reader = in.createTailer().methodReader(orderManager);
                for (int i = 0; i < 5; i++)
                    assertTrue(reader.readOne());

                assertFalse(reader.readOne());
                // System.out.println(out.dump());
            }

            try (ChronicleQueue in = ChronicleQueue.singleBuilder(queuePath2).testBlockSize().sourceId(2).build()) {
                MethodReader reader = in.createTailer().methodReader((OrderListener) order -> {
                    MessageHistory x = MessageHistory.get();
                    // Note: this will have one extra timing, the time it was written to the console.
                    // System.out.println(x);
                    assertEquals(1, x.sourceId(0));
                    assertEquals(2, x.sourceId(1));
                    assertEquals(4, x.timings());
                });
                assertTrue(reader.readOne());
                assertFalse(reader.readOne());
            }
        } finally {
            try {
                IOTools.shallowDeleteDirWithFiles(queuePath);
                IOTools.shallowDeleteDirWithFiles(queuePath2);
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testRestartingAService() {
//        expectException("Overriding roll length from existing metadata");
//        expectException("Overriding roll cycle from");

        File queuePath = DirectoryUtils.tempDir("testRestartingAService");
        File queuePath2 = DirectoryUtils.tempDir("testRestartingAService-down");
        try {

            try (ChronicleQueue out = ChronicleQueue.singleBuilder(queuePath)
                    .testBlockSize()
                    .rollCycle(RollCycles.TEST_DAILY)
                    .sourceId(1)
                    .build()) {
                SidedMarketDataListener combiner = out.acquireAppender()
                        .methodWriterBuilder(SidedMarketDataListener.class)
                        .get();

                combiner.onSidedPrice(new SidedPrice("EURUSD1", 123456789000L, Side.Sell, 1.1172, 2e6));
                combiner.onSidedPrice(new SidedPrice("EURUSD2", 123456789100L, Side.Buy, 1.1160, 2e6));

                for (int i = 2; i < 10; i += 2) {
                    combiner.onSidedPrice(new SidedPrice("EURUSD3", 123456789100L, Side.Sell, 1.1173, 2.5e6));
                    combiner.onSidedPrice(new SidedPrice("EURUSD4", 123456789100L, Side.Buy, 1.1167, 1.5e6));
                }
            }
//            DumpQueueMain.dump(queuePath.getAbsolutePath());

            // TODO FIx for more.
            for (int i = 0; i < 10; i++) {
                // read one message at a time
                try (ChronicleQueue in = ChronicleQueue.singleBuilder(queuePath)
                        .testBlockSize()
                        .sourceId(1)
                        .build();
                     ChronicleQueue out = ChronicleQueue.singleBuilder(queuePath2)
                             .testBlockSize()
                             .sourceId(1)
                             .rollCycle(RollCycles.TEST_DAILY)
                             .build()) {

                    ExcerptAppender excerptAppender = out.acquireAppender();
                    MarketDataListener mdListener = excerptAppender
                            .methodWriterBuilder(MarketDataListener.class)
                            .get();

                    SidedMarketDataCombiner combiner = new SidedMarketDataCombiner(mdListener);
                    ExcerptTailer tailer = in.createTailer("test");
                    assertEquals("tailer.index()=" + Long.toHexString(tailer.index()), i, in.rollCycle().toSequenceNumber(tailer.index()));
                    MethodReader reader = tailer
                            .methodReader(combiner);

                    // System.out.println("#### IN\n" + in.dump());
                    // System.out.println("#### OUT:\n" + out.dump());
                    assertTrue("i: " + i, reader.readOne());
                }
            }
        } finally {
            try {
                IOTools.shallowDeleteDirWithFiles(queuePath);

            } catch (Exception ignore) {
            }

            try {

                IOTools.shallowDeleteDirWithFiles(queuePath2);
            } catch (Exception ignore) {
            }
        }
    }
}