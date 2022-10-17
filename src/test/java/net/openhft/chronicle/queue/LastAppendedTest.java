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

package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MessageHistory;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST4_SECONDLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RequiredForClient
public class LastAppendedTest extends QueueTestCommon {

    public static final RollCycle ROLL_CYCLE = TEST4_SECONDLY;

    @Test
    public void testLastWritten() {
        SetTimeProvider timeProvider = new SetTimeProvider(0).advanceMillis(1000);

        final File outQueueDir = getTmpDir();
        final File inQueueDir = getTmpDir();

        try (ChronicleQueue outQueue = single(outQueueDir).rollCycle(ROLL_CYCLE).sourceId(1).timeProvider(timeProvider).build();
             ChronicleQueue inQueue = single(inQueueDir).rollCycle(ROLL_CYCLE).sourceId(2).timeProvider(timeProvider).build()) {

            // write some initial data to the inqueue
            final LATMsg msg = inQueue.acquireAppender()
                    .methodWriterBuilder(LATMsg.class)
                    .get();

            msg.msg("somedata-0");

            timeProvider.advanceMillis(1000);

            // write data into the inQueue
            msg.msg("somedata-1");

            // read a message on the in queue and write it to the out queue
            {
                LATMsg out = outQueue.acquireAppender()
                        .methodWriterBuilder(LATMsg.class)
                        .get();
                MethodReader methodReader = inQueue.createTailer()
                        .methodReader((LATMsg) out::msg);

                // reads the somedata-0
                methodReader.readOne();

                // reads the somedata-1
                methodReader.readOne();
            }

            // write data into the inQueue
            msg.msg("somedata-2");

            timeProvider.advanceMillis(2000);

            msg.msg("somedata-3");
            msg.msg("somedata-4");


            final AtomicReference<String> actualValue = new AtomicReference<>();

            // check that we are able to pick up from where we left off, in other words the next read should be somedata-2
            {
                ExcerptTailer excerptTailer = inQueue.createTailer().afterLastWritten(outQueue);
                long index = excerptTailer.index();
                MethodReader methodReader = excerptTailer.methodReader((LATMsg) actualValue::set);

                methodReader.readOne();
                try {
                    assertEquals("somedata-2", actualValue.get());
                    methodReader.readOne();
                    assertEquals("somedata-3", actualValue.get());
                    methodReader.readOne();
                    assertEquals("somedata-4", actualValue.get());
                } catch (AssertionError ae) {
                    System.out.println("index: " + Long.toHexString(index));
                    System.out.println("inQueue");
                    System.out.println(inQueue.dump());
                    System.out.println("outQueue");
                    System.out.println(outQueue.dump());
                    throw ae;
                }
            }
        }
    }

    @Test
    public void testLastWrittenMetadata0() {
        SetTimeProvider timeProvider = new SetTimeProvider(0).advanceMillis(1000);

        final File outQueueDir = getTmpDir();
        final File inQueueDir = getTmpDir();

        try (ChronicleQueue outQueue = single(outQueueDir).rollCycle(ROLL_CYCLE).sourceId(1).timeProvider(timeProvider).build();
             ChronicleQueue inQueue = single(inQueueDir).rollCycle(ROLL_CYCLE).sourceId(2).timeProvider(timeProvider).build()) {


            // write some initial data to the inqueue
            final LATMsg msg = inQueue.acquireAppender()
                    .methodWriterBuilder(LATMsg.class)
                    .get();

            msg.msg("somedata-0");
            msg.msg("somedata-1");

            // read a message on the in queue and write it to the out queue
            {
                LATMsg out = outQueue.acquireAppender()
                        .methodWriterBuilder(LATMsg.class)
                        .get();
                MethodReader methodReader = inQueue.createTailer().methodReader((LATMsg) out::msg);

                // reads the somedata-0
                methodReader.readOne();

                // reads the somedata-1
                methodReader.readOne();
            }

            // write data into the inQueue
            msg.msg("somedata-2");
            msg.msg("somedata-3");
            msg.msg("somedata-4");

            try (DocumentContext dc = outQueue.acquireAppender().writingDocument(true)) {
                dc.wire().write("some metadata");
            }

            AtomicReference<String> actualValue = new AtomicReference<>();

            // check that we are able to pick up from where we left off, in other words the next read should be somedata-2
            {
                ExcerptTailer excerptTailer = inQueue.createTailer().afterLastWritten(outQueue);
                long index = excerptTailer.index();
                try (DocumentContext dc = excerptTailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    MessageHistory mh = dc.wire().read("history").object(MessageHistory.get(), MessageHistory.class);
                    assertTrue(mh.toString(), mh.toString().startsWith("VanillaMessageHistory{sources: [2=0x100000002] timings: "));
                    final String msg0 = dc.wire().readEvent(String.class);
                    assertEquals("msg", msg0);
                    Object o = dc.wire().getValueIn().object();
                    assertEquals("somedata-2", o);
                }
                assertTrue(excerptTailer.moveToIndex(index));
                MethodReader methodReader = excerptTailer.methodReader((LATMsg) actualValue::set);
                try {
                    methodReader.readOne();
                    assertEquals("somedata-2", actualValue.get());
                    methodReader.readOne();
                    assertEquals("somedata-3", actualValue.get());
                    methodReader.readOne();
                    assertEquals("somedata-4", actualValue.get());
                } catch (AssertionError ae) {
                    System.out.println("index: " + Long.toHexString(index));
                    System.out.println("inQueue");
                    System.out.println(inQueue.dump());
                    System.out.println("outQueue");
                    System.out.println(outQueue.dump());
                    throw ae;
                }
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testLastWrittenToStartToEnd() {
        SetTimeProvider timeProvider = new SetTimeProvider(0).advanceMillis(1000);

        final File outQueueDir = getTmpDir();
        final File inQueueDir = getTmpDir();

        try (ChronicleQueue outQueue = single(outQueueDir).rollCycle(ROLL_CYCLE).sourceId(1).timeProvider(timeProvider).build();
             ChronicleQueue inQueue = single(inQueueDir).rollCycle(ROLL_CYCLE).sourceId(2).timeProvider(timeProvider).build()) {

            // write some initial data to the inqueue
            final LATMsg msg = inQueue.acquireAppender()
                    .methodWriterBuilder(LATMsg.class)
                    .get();

            msg.msg("somedata-0");
            msg.msg("somedata-1");

            // read a message on the in queue and write it to the out queue
            {
                LATMsg out = outQueue.acquireAppender()
                        .methodWriterBuilder(LATMsg.class)
                        .get();
                MethodReader methodReader = inQueue.createTailer().methodReader((LATMsg) out::msg);

                // reads the somedata-0
                methodReader.readOne();

                // reads the somedata-1
                methodReader.readOne();
            }

            // write data into the inQueue
            msg.msg("somedata-2");
            msg.msg("somedata-3");
            msg.msg("somedata-4");

            try (DocumentContext dc = outQueue.acquireAppender().writingDocument(true)) {
                dc.wire().write("some metadata");
            }

            AtomicReference<String> actualValue = new AtomicReference<>();

            // check that we are able to pick up from where we left off, in other words the next read should be somedata-2
            {
                ExcerptTailer excerptTailer = inQueue.createTailer().afterLastWritten(outQueue);
                long index = excerptTailer.index();
                try (DocumentContext dc = excerptTailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    excerptTailer.toStart();
                    excerptTailer.toEnd();
                }
            }
        }
    }
}
