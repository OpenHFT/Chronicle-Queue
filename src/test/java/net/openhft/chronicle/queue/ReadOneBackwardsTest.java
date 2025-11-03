/*
 * Copyright 2016-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.VanillaMessageHistory;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.*;

/**
 * test reading the queue backwards using readOne
 */
public class ReadOneBackwardsTest extends QueueTestCommon {

    @Test
    public void test() {
        doTest(false);
    }

    @Test
    public void testScanning() {
        doTest(true);
    }

    private void doTest(boolean scanning) {

        final BlockingQueue<SnapshotDTO> blockingQueue = new ArrayBlockingQueue<>(128);

        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(getTmpDir()).sourceId(1).build()) {

            MyDtoListener myOut = q.methodWriter(MyDtoListener.class);
            SnapshotListener snapshotOut = q.methodWriter(SnapshotListener.class);

            generateHistory(1);
            myOut.myDto(new MyDto());

            generateHistory(2);
            snapshotOut.snapshot(new SnapshotDTO("data"));

            generateHistory(3);
            myOut.myDto(new MyDto());

            generateHistory(4);
            myOut.myDto(new MyDto());

            ExcerptTailer tailer = q.createTailer().toEnd().direction(TailerDirection.BACKWARD);
            MethodReader reader = tailer.methodReaderBuilder()
                    .scanning(scanning)
                    .warnMissing(false)
                    .build((SnapshotListener) blockingQueue::add);

            if (!scanning) {
                assertTrue(reader.readOne());
                assertTrue(reader.readOne());
            }

            assertTrue(blockingQueue.isEmpty());
            assertTrue(reader.readOne());

            SnapshotDTO snapshotDTO = blockingQueue.poll();
            assertNotNull(snapshotDTO);
            assertEquals("data", snapshotDTO.data);

            if (!scanning)
                assertTrue(reader.readOne());

            assertFalse(reader.readOne());
        }
    }

    @NotNull
    private VanillaMessageHistory generateHistory(int value) {
        VanillaMessageHistory messageHistory = (VanillaMessageHistory) MessageHistory.get();
        messageHistory.reset();
        messageHistory.addSource(value, value);
        return messageHistory;
    }

    interface MyDtoListener {
        void myDto(MyDto dto);
    }

    interface SnapshotListener {
        void snapshot(SnapshotDTO dto);
    }

    static class MyDto extends SelfDescribingMarshallable {
        String data;
    }

    static class SnapshotDTO extends SelfDescribingMarshallable {
        String data;

        SnapshotDTO(String data) {
            this.data = data;
        }
    }
}
