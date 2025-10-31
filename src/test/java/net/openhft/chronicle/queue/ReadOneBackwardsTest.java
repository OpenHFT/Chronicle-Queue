/*
 * Copyright 2016-2025 chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    public void doTest(boolean scanning) {

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

        public SnapshotDTO(String data) {
            this.data = data;
        }
    }
}
