/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.VanillaMessageHistory;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * test reading the queue backwards using readOne
 */
public class ReadOneBackwardsTest extends QueueTestCommon {

    @Test
    public void test() {
        SnapshotDTO snapshotDTO = doTest(false);
        assertNotNull(snapshotDTO, "Snapshot should be successfully read when reading queue backwards");
        assertEquals("data", snapshotDTO.data, "Snapshot data should match expected value when reading backwards");
    }

    @Test
    public void testScanning() {
        SnapshotDTO snapshotDTO = doTest(true);
        assertNotNull(snapshotDTO, "Snapshot should be successfully read when scanning queue backwards");
        assertEquals("data", snapshotDTO.data, "Snapshot data should match expected value when scanning backwards");
    }

    private SnapshotDTO doTest(boolean scanning) {

        final BlockingQueue<SnapshotDTO> blockingQueue = new ArrayBlockingQueue<>(128);

        SnapshotDTO snapshotDTO;
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(getTmpDir()).sourceId(1).build()) {

            MyDtoListener myOut = q.methodWriter(MyDtoListener.class);
            final SnapshotListener snapshotOut = q.methodWriter(SnapshotListener.class);

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
                assertTrue(reader.readOne(), "MethodReader should read first DTO when reading backwards in non-scanning mode");
                assertTrue(reader.readOne(), "MethodReader should read snapshot when reading backwards in non-scanning mode");
            }

            assertTrue(blockingQueue.isEmpty(), "Snapshot should not yet be delivered to blocking queue before final readOne");
            assertTrue(reader.readOne(), "MethodReader should successfully deliver snapshot to listener when reading backwards");

            snapshotDTO = blockingQueue.poll();

            if (!scanning)
                assertTrue(reader.readOne(), "MethodReader should read second DTO when reading backwards in non-scanning mode");

            assertFalse(reader.readOne(), "MethodReader should reach end of queue when reading backwards");
        }
        return snapshotDTO;
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
        final String data;

        SnapshotDTO(String data) {
            this.data = data;
        }
    }
}
