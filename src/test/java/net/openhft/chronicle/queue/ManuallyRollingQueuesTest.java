package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.Math.*;

public class ManuallyRollingQueuesTest {

    public static final int MAX_COUNT = 10_000;
    private final SetTimeProvider tp1 = new SetTimeProvider();
    private final SetTimeProvider tp2 = new SetTimeProvider();


    /**
     * This first test checks what happened if you were to create two threads to the same queue directory for each queue
     * you set it up with a time provider, and where those time providers are not in sync.
     *
     * @throws IOException
     */
    @Ignore("Currently failing")
    @Test
    public void test() throws IOException {

        final Path qFolder = Files.createTempDirectory("chron");

        try (SingleChronicleQueue q1 = SingleChronicleQueueBuilder.binary(qFolder).rollCycle(RollCycles.TEST_SECONDLY).timeProvider(tp1).build();
             SingleChronicleQueue q2 = SingleChronicleQueueBuilder.binary(qFolder).rollCycle(RollCycles.TEST_SECONDLY).timeProvider(tp2).build();
             ExcerptAppender append1 = q1.acquireAppender();
             ExcerptAppender append2 = q2.acquireAppender()) {

            tp1.advanceMillis(1_000);
            append1.writeText("hello 1");

            tp2.advanceMillis(5_000);
            append2.writeText("hello 2");

            tp1.advanceMillis(1_000);
            append1.writeText("hello 3");

            tp2.advanceMillis(5_000);
            append2.writeText("hello 4");

            ExcerptTailer tailer = q1.createTailer();
            System.out.println(tailer.readText());
            System.out.println(tailer.readText());
            System.out.println(tailer.readText());
            System.out.println(tailer.readText());

        }
    }

    private Path qFolder;

    /**
     * This test case is a little more realistic as it uses a single-time provider, but write the messages in order
     * using two instances of chronicle queue pointing to the same queue directory.
     * <p>
     * It sets up a number of threads each with its own instance of chronicle queue to check that all the numbers are
     * read in order, with no gaps.
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Ignore("Currently failing")
    @Test
    public void test2() throws IOException, ExecutionException, InterruptedException {

        qFolder = Files.createTempDirectory("chron");
        final SetTimeProvider tp = new SetTimeProvider();

        try (SingleChronicleQueue q1 = SingleChronicleQueueBuilder.binary(qFolder).rollCycle(RollCycles.TEST_SECONDLY).timeProvider(tp).build();
             SingleChronicleQueue q2 = SingleChronicleQueueBuilder.binary(qFolder).rollCycle(RollCycles.TEST_SECONDLY).timeProvider(tp).build();
             ExcerptAppender append1 = q1.acquireAppender();
             ExcerptAppender append2 = q2.acquireAppender()) {

            Future<Void> f1 = Executors.newSingleThreadExecutor().submit(this::checkOrderOfMessages);
            Future<Void> f2 = Executors.newSingleThreadExecutor().submit(this::checkOrderOfMessages);
            Future<Void> f3 = Executors.newSingleThreadExecutor().submit(this::checkOrderOfMessages);

            for (int count = 0; count < MAX_COUNT; count++) {
                tp.advanceMillis((long) (random() * 500));
                ExcerptAppender a = random() > 0.5 ? append1 : append2;
                a.writeMessage("value", count);
            }

            f1.get();
            f2.get();
            f3.get();
        }
    }

    private Void checkOrderOfMessages() {
        int count = 0;
        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(qFolder).build();
             ExcerptTailer tailer = q.createTailer()) {
            while (count < MAX_COUNT) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        continue;
                    Assert.assertEquals(count++, (int) dc.wire().read("value").object());
                }
            }
        }
        return null;
    }

}
