package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.testframework.FlakyTestRunner;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static net.openhft.chronicle.testframework.FlakyTestRunner.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@RequiredForClient
public class LastAppendedTest extends ChronicleQueueTestBase {
   // @Test
   // public void testLastWritten() {
    //    builder(this::testLastWritten0).build().run();
   // }


   // public void testLastWrittenMetadata() {
  //      builder(this::testLastWrittenMetadata0).build().run();
  //  }
    @Test
    void testLastWritten0() {
        SetTimeProvider timeProvider = new SetTimeProvider();

        final File outQueueDir = getTmpDir();
        final File inQueueDir = getTmpDir();

        try (ChronicleQueue outQueue = single(outQueueDir).rollCycle(RollCycles.TEST_SECONDLY).sourceId(1).timeProvider(timeProvider).build();
             ChronicleQueue inQueue = single(inQueueDir).rollCycle(RollCycles.TEST_SECONDLY).sourceId(2).timeProvider(timeProvider).build()) {

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
                    final String actual = actualValue.get();
                    try {
                        Assert.assertEquals("somedata-2", actual);
                        methodReader.readOne();
                        Assert.assertEquals("somedata-3", actualValue.get());
                        methodReader.readOne();
                        Assert.assertEquals("somedata-4", actualValue.get());
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
    void testLastWrittenMetadata0() {
        SetTimeProvider timeProvider = new SetTimeProvider().advanceMillis(1000);

        final File outQueueDir = getTmpDir();
        final File inQueueDir = getTmpDir();

        try (ChronicleQueue outQueue = single(outQueueDir).rollCycle(RollCycles.TEST_SECONDLY).sourceId(1).timeProvider(timeProvider).build();
             ChronicleQueue inQueue = single(inQueueDir).rollCycle(RollCycles.TEST_SECONDLY).sourceId(2).timeProvider(timeProvider).build()) {


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

                MethodReader methodReader = excerptTailer.methodReader((LATMsg) actualValue::set);

                methodReader.readOne();
                assertEquals("somedata-2", actualValue.get());

                methodReader.readOne();
                assertEquals("somedata-3", actualValue.get());

                methodReader.readOne();
                assertEquals("somedata-4", actualValue.get());
            }
        }
    }
}

