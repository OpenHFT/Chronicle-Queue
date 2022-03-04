package net.openhft.chronicle.queue.impl.single.pretoucher;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.QueueTestCommon;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.MessageHistory;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.Ignore;

import java.util.UUID;

@Ignore("stress test")
public class PretoucherSoakTest extends QueueTestCommon {

    public static void main(String[] args) {
        SingleChronicleQueue outQueue = SingleChronicleQueueBuilder.binary("target/" + "monitor")
                .rollCycle(RollCycles.TEST_SECONDLY).build();
        ExcerptAppender outQueueAppender = outQueue.acquireAppender();

        HeartbeatListener heartbeatWriter = outQueueAppender.methodWriterBuilder(HeartbeatListener.class).updateInterceptor((m, a) -> {
            ValidFields.validateAll(a);
            return true;
        }).get();

        long periodicUpdateUS = (long) 10 * 1000;

        long lastHB = 0;
        while (true) {
            if (System.currentTimeMillis() - lastHB > 1) {
                // write a hb to the queue
                MessageHistory.get().reset();
                Heartbeat heartBeat = new Heartbeat(UUID.randomUUID().toString());
                heartbeatWriter.heartbeat(heartBeat);
                lastHB = System.currentTimeMillis();
            }
        }
    }

    public interface HeartbeatListener {

        /**
         * called periodically under normal operation
         *
         */
        void heartbeat(Heartbeat heartbeat);

    }

    public static class Heartbeat extends SelfDescribingMarshallable implements Validatable {
        final String source;
        long time;

        public Heartbeat(String source) {
            this.source = source;
        }

        public String source() {
            return source;
        }

        public long time() {
            return time;
        }

        public Heartbeat time(long time) {
            this.time = time;
            return this;
        }

        @Override
        public void validate() throws IllegalStateException {

        }
    }
}