/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.RequiredForClient;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.time.SetTimeProvider;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static net.openhft.chronicle.queue.rollcycles.TestRollCycles.TEST_SECONDLY;
import static org.junit.jupiter.api.Assertions.*;

@RequiredForClient
@SuppressWarnings({"deprecation", "removal"})
public class LastAcknowledgedTest extends QueueTestCommon {
    @Test
    public void testLastAcknowledge() {
        String name = OS.getTarget() + "/testLastAcknowledge-" + Time.uniqueId();
        long lastIndexAppended;
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(name).testBlockSize().build();
             ExcerptAppender excerptAppender = q.createAppender()) {
            excerptAppender.writeText("Hello World");
            lastIndexAppended = excerptAppender.lastIndexAppended();

            ExcerptTailer tailer = q.createTailer();
            try (DocumentContext dc = tailer.readingDocument()) {
                assertTrue(dc.isData(), "last acknowledged: first document is data");
                assertEquals(lastIndexAppended, tailer.index(), "last acknowledged: tailer index");
            }

            ExcerptTailer tailer2 = q.createTailer();
            tailer2.readAfterReplicaAcknowledged(true);
            try (DocumentContext dc = tailer2.readingDocument()) {
                assertFalse(dc.isPresent(), "last acknowledged: not present until acknowledged");
            }
        }
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(name).testBlockSize().build()) {
            assertEquals(-1, q.lastAcknowledgedIndexReplicated(), "last acknowledged: initial replicated index");

            q.lastAcknowledgedIndexReplicated(lastIndexAppended - 1);

            ExcerptTailer tailer2 = q.createTailer();
            tailer2.readAfterReplicaAcknowledged(true);
            try (DocumentContext dc = tailer2.readingDocument()) {
                assertFalse(dc.isPresent(), "last acknowledged: not present at lastIndexAppended - 1");
            }

            q.lastAcknowledgedIndexReplicated(lastIndexAppended);

            try (DocumentContext dc = tailer2.readingDocument()) {
                assertTrue(dc.isData(), "last acknowledged: present after acknowledgement");
                assertEquals(lastIndexAppended, tailer2.index(), "last acknowledged: tailer2 index");
            }
        }
        IOTools.deleteDirWithFiles(name);
    }

    @Test
    public void testReadBeforeAcknowledgment() throws IOException {

        // Set up a Chronicle Queue and a StoreTailer for testing
        String pathName = "target" + System.nanoTime();
        Path tempDirectory = Files.createTempDirectory(pathName);

        try (ChronicleQueue queue = ChronicleQueue.single(tempDirectory.toFile().getAbsolutePath())) {
            LongValue lastAcknowledgedIndexReplicatedLongValue = Jvm.getValue(queue, "lastAcknowledgedIndexReplicated");
            ExcerptAppender appender = queue.createAppender();

            ExcerptTailer tailer = queue.createTailer();
            Assertions.assertFalse(tailer.readAfterReplicaAcknowledged(), "read before ack: default readAfterReplicaAcknowledged");

            // Set up the tailer to use a custom acknowledged index replicated check
            tailer.acknowledgedIndexReplicatedCheck((index, lastSequenceAck) -> index <= lastSequenceAck);
            Assertions.assertTrue(tailer.readAfterReplicaAcknowledged(), "read before ack: readAfterReplicaAcknowledged enabled");

            // tolerateNumberOfUnAckedMessages
            {
                appender.writeText("hello1");
                assertNull(tailer.readText(), "read before ack: hello1 not yet visible");
                lastAcknowledgedIndexReplicatedLongValue.setVolatileValue(appender.lastIndexAppended());
                Assertions.assertEquals("hello1", tailer.readText(), "read before ack: hello1 visible");
                assertNull(tailer.readText(), "read before ack: end after hello1");
            }

            // tolerateNumberOfUnAckedMessages = 1
            {
                int tolerateNumberOfUnAckedMessages = 1;
                tailer.acknowledgedIndexReplicatedCheck((index, lastSequenceAck) -> index <= lastSequenceAck + tolerateNumberOfUnAckedMessages);
                appender.writeText("hello2");
                lastAcknowledgedIndexReplicatedLongValue.setVolatileValue(appender.lastIndexAppended());
                appender.writeText("hello3");
                Assertions.assertEquals("hello2", tailer.readText(), "read before ack: hello2 visible");
                Assertions.assertEquals("hello3", tailer.readText(), "read before ack: hello3 visible");
                assertNull(tailer.readText(), "read before ack: end after hello3");
            }

            // tolerateNumberOfUnAckedMessages = 2
            {
                int tolerateNumberOfUnAckedMessages = 2;
                tailer.acknowledgedIndexReplicatedCheck((index, lastSequenceAck) -> index <= lastSequenceAck + tolerateNumberOfUnAckedMessages);
                appender.writeText("hello4");
                lastAcknowledgedIndexReplicatedLongValue.setVolatileValue(appender.lastIndexAppended());
                appender.writeText("hello5");
                appender.writeText("hello6");
                Assertions.assertEquals("hello4", tailer.readText(), "read before ack: hello4 visible");
                Assertions.assertEquals("hello5", tailer.readText(), "read before ack: hello5 visible");
                Assertions.assertEquals("hello6", tailer.readText(), "read before ack: hello6 visible");
                assertNull(tailer.readText(), "read before ack: end after hello6");
            }
        }
    }

    /**
     * Tests than on a roll, we don't allow any in flight messages when the in flight message is from a different cycle,
     * ( During a roll all messages must be acknowledged before they are seen ) - In other words in flight messages
     * from a different cycle are not currently supported, because it difficult to track the number of messages in
     * each roll cycle. Later it won't be impossible to add support for this in the future but given there is usually a natural stall
     * all role anyway, it is not a high priority.
     *
     * @throws IOException if the Chronicle Queue cannot be created
     */
    @Test
    public void testReadBeforeAcknowledgmentOnRoll() throws IOException {

        // Set up a Chronicle Queue and a StoreTailer for testing
        String pathName = "target" + System.nanoTime();
        Path tempDirectory = Files.createTempDirectory(pathName);

        SetTimeProvider timeProvider = new SetTimeProvider();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tempDirectory.toFile().getAbsolutePath())
                .timeProvider(timeProvider)
                .rollCycle(TEST_SECONDLY).build()) {
            LongValue lastAcknowledgedIndexReplicatedLongValue = Jvm.getValue(queue, "lastAcknowledgedIndexReplicated");
            ExcerptAppender appender = queue.createAppender();
            timeProvider.set(1);
            ExcerptTailer tailer = queue.createTailer();
            Assertions.assertFalse(tailer.readAfterReplicaAcknowledged(), "read before ack roll: default readAfterReplicaAcknowledged");

            // Set up the tailer to use a custom acknowledged index replicated check
            tailer.acknowledgedIndexReplicatedCheck((index, lastSequenceAck) -> index <= lastSequenceAck);
            Assertions.assertTrue(tailer.readAfterReplicaAcknowledged(), "read before ack roll: readAfterReplicaAcknowledged enabled");

            timeProvider.set(1);
            // tolerateNumberOfUnAckedMessages
            {
                appender.writeText("hello1");
                appender.writeText("hello2");
                assertNull(tailer.readText(), "read before ack roll: hello1 not yet visible");
                lastAcknowledgedIndexReplicatedLongValue.setVolatileValue(appender.lastIndexAppended());
                Assertions.assertEquals("hello1", tailer.readText(), "read before ack roll: hello1 visible");
                Assertions.assertEquals("hello2", tailer.readText(), "read before ack roll: hello2 visible");
                assertNull(tailer.readText(), "read before ack roll: end after hello2");
            }

            timeProvider.set(2);
            appender.writeText("hello3");
            lastAcknowledgedIndexReplicatedLongValue.setVolatileValue(appender.lastIndexAppended());
            appender.writeText("hello4");

            // causing the roll
            timeProvider.set(1002);

            Assertions.assertEquals("hello3", tailer.readText(), "read before ack roll: hello3 visible");
            assertNull(tailer.readText(), "read before ack roll: end after hello3");
        }
    }
}
