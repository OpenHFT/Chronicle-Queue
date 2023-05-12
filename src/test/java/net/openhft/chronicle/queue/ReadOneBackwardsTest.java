package net.openhft.chronicle.queue;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.VanillaMessageHistory;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * test reading the queue backwards using readOne
 */
public class ReadOneBackwardsTest extends QueueTestCommon {

    static class MyDto extends SelfDescribingMarshallable {
        String data;
    }

    interface MyDtoListener {
        void myDto(MyDto dto);
    }


    static class SnapshotDTO extends SelfDescribingMarshallable {
        String data;

        public SnapshotDTO(String data) {
            this.data = data;
        }
    }

    interface SnapshotListener {
        void snapshot(SnapshotDTO dto);
    }


    @Test
    public void test() throws InterruptedException {

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


            //    System.out.println(q.dump());

            ExcerptTailer tailer = q.createTailer().toEnd().direction(TailerDirection.BACKWARD);
            boolean success = tailer.methodReaderBuilder().warnMissing(false).build(new SnapshotListener() {
                @Override
                public void snapshot(SnapshotDTO e) {
                    blockingQueue.add(e);
                }
            }).readOne();


            Assert.assertTrue(success);

            SnapshotDTO snapshotDTO = blockingQueue.poll();
            Assert.assertNotNull(snapshotDTO);
            Assert.assertEquals("data", snapshotDTO.data);
        }
    }

    @NotNull
    private VanillaMessageHistory generateHistory(int value) {
        VanillaMessageHistory messageHistory = (VanillaMessageHistory) MessageHistory.get();
        messageHistory.reset();
        messageHistory.addSource(value, value);
        return messageHistory;
    }

}

