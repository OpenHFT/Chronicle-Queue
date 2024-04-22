package net.openhft.chronicle.queue.impl.single;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AcknowledgedIndexReplicatedCheckTest {

    @Test
    public void testReadBeforeAcknowledgment() throws IOException {

        // Set up a Chronicle Queue and a StoreTailer for testing
        String pathName = "target" + System.nanoTime();
        Path tempDirectory = Files.createTempDirectory(pathName);

        try (ChronicleQueue queue = ChronicleQueue.single(tempDirectory.toFile().getAbsolutePath())) {
            LongValue lastAcknowledgedIndexReplicatedLongValue = Jvm.getValue(queue, "lastAcknowledgedIndexReplicated");
            ExcerptAppender appender = queue.acquireAppender();

            ExcerptTailer tailer = queue.createTailer();
            Assert.assertFalse(tailer.readAfterReplicaAcknowledged());

            // Set up the tailer to use a custom acknowledged index replicated check
            tailer.acknowledgedIndexReplicatedCheck((index, lastSequenceAck) -> index <= lastSequenceAck);
            Assert.assertTrue(tailer.readAfterReplicaAcknowledged());

            // tolerateNumberOfUnAckedMessages
            {
                appender.writeText("hello1");
                Assert.assertEquals(null, tailer.readText());
                lastAcknowledgedIndexReplicatedLongValue.setVolatileValue(appender.lastIndexAppended());
                Assert.assertEquals("hello1", tailer.readText());
                Assert.assertEquals(null, tailer.readText());
            }

            // tolerateNumberOfUnAckedMessages = 1
            {
                int tolerateNumberOfUnAckedMessages = 1;
                tailer.acknowledgedIndexReplicatedCheck((index, lastSequenceAck) -> index <= lastSequenceAck + tolerateNumberOfUnAckedMessages);
                appender.writeText("hello2");
                lastAcknowledgedIndexReplicatedLongValue.setVolatileValue(appender.lastIndexAppended());
                appender.writeText("hello3");
                Assert.assertEquals("hello2", tailer.readText());
                Assert.assertEquals("hello3", tailer.readText());
                Assert.assertEquals(null, tailer.readText());
            }

            // tolerateNumberOfUnAckedMessages = 2
            {
                int tolerateNumberOfUnAckedMessages = 2;
                tailer.acknowledgedIndexReplicatedCheck((index, lastSequenceAck) -> index <= lastSequenceAck + tolerateNumberOfUnAckedMessages);
                appender.writeText("hello4");
                lastAcknowledgedIndexReplicatedLongValue.setVolatileValue(appender.lastIndexAppended());
                appender.writeText("hello5");
                appender.writeText("hello6");
                Assert.assertEquals("hello4", tailer.readText());
                Assert.assertEquals("hello5", tailer.readText());
                Assert.assertEquals("hello6", tailer.readText());
                Assert.assertEquals(null, tailer.readText());
            }
        }
    }
}

